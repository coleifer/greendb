#!/usr/bin/env python

import gevent
from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from gevent.thread import get_ident

import lmdb
import msgpack

from collections import namedtuple
from contextlib import contextmanager
from functools import wraps
from io import BytesIO
from socket import error as socket_error
import datetime
import json
import logging
import operator
import optparse
import os
import shutil
import sys
import struct
import time


__version__ = '0.1.0'

logger = logging.getLogger(__name__)


if sys.version_info[0] == 3:
    unicode = str
    basestring = (bytes, str)
    int_types = int
else:
    int_types = (int, long)

def encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    elif isinstance(s, bytes):
        return s
    else:
        return str(s).encode('utf-8')

def decode(s):
    if isinstance(s, unicode):
        return s
    elif isinstance(s, bytes):
        return s.decode('utf-8')
    else:
        return str(s)


CRLF = b'\r\n'
READSIZE = 4 * 1024


class ClientQuit(Exception): pass
class Shutdown(Exception): pass

class ServerError(Exception): pass
class ConnectionError(ServerError): pass
class ServerInternalError(ServerError): pass

class CommandError(Exception):
    def __init__(self, message):
        self.message = message
        super(CommandError, self).__init__()
    def __str__(self):
        return self.message
    __unicode__ = __str__


Error = namedtuple('Error', ('message',))


class _Socket(object):
    def __init__(self, s):
        self._socket = s
        self.is_closed = False
        self.buf = BytesIO()
        self.bytes_read = self.bytes_written = 0
        self.recvbuf = bytearray(READSIZE)

        self.sendbuf = BytesIO()
        self.bytes_pending = 0
        self._write_pending = False

    def __del__(self):
        if not self.is_closed:
            self.buf.close()
            self.sendbuf.close()
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()

    def _read_from_socket(self, length):
        l = marker = 0
        recvptr = memoryview(self.recvbuf)
        self.buf.seek(self.bytes_written)

        try:
            while True:
                # Read up to READSIZE bytes into our dedicated recvbuf (using
                # buffer protocol to avoid copy).
                l = self._socket.recv_into(recvptr, READSIZE)
                if not l:
                    self.close()
                    raise ConnectionError('client went away')

                # Copy bytes read into file-like socket buffer.
                self.buf.write(recvptr[:l])
                self.bytes_written += l

                marker += l
                if length > 0 and length > marker:
                    continue
                break
        except socket.timeout:
            raise ConnectionError('timed out reading from socket')
        except socket.error:
            raise ConnectionError('error while reading from socket')

    def _read(self, length):
        buflen = self.bytes_written - self.bytes_read

        # Are we requesting more data than is available in the socket buffer?
        # If so, read from the socket into our socket buffer. This operation
        # may block or fail if the sender disconnects.
        if length > buflen:
            self._read_from_socket(length - buflen)

        # Move the cursor to the last-read byte of the socket buffer and read
        # the requested length. Update the last-read byte for subsequent reads.
        self.buf.seek(self.bytes_read)
        data = self.buf.read(length)
        self.bytes_read += length

        # If we have read all the data available in the socket buffer, truncate
        # so that it does not grow endlessly.
        if self.bytes_read == self.bytes_written:
            self.purge()
        return data

    def read(self, length):
        # Convenience function for reading a number of bytes followed by CRLF.
        # The CRLF is thrown away but will appear to have been read/consumed.
        data = self._read(length)
        self._read(2)  # Read the CR/LF... assert self._read(2) == CRLF.
        return data

    def readline(self):
        # Move the cursor to the last-read byte of the socket buffer and
        # attempt to read up to the first CRLF.
        self.buf.seek(self.bytes_read)
        data = self.buf.readline()

        # If the data did not end with a CRLF, then get more from the socket
        # until we can read a line.
        while not data.endswith(CRLF):
            self._read_from_socket(0)
            self.buf.seek(self.bytes_read)
            data = self.buf.readline()

        # Update the last-read byte, marking our line as having been read.
        self.bytes_read += len(data)

        # If we've read all the available data in the socket buffer, truncate
        # so that it does not grow endlessly.
        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]  # Strip CRLF.

    def write(self, data):
        self.bytes_pending += self.sendbuf.write(data)

    def send(self, blocking=False):
        if self.bytes_pending and not self._write_pending:
            self._write_pending = True
            if blocking:
                self._send_to_socket()
            else:
                gevent.spawn(self._send_to_socket)
            return True
        return False

    def _send_to_socket(self):
        data = self.sendbuf.getvalue()
        self.sendbuf.seek(0)
        self.sendbuf.truncate()
        self.bytes_pending = 0
        try:
            self._socket.sendall(data)
        except socket.error:
            self.close()
            raise ConnectionError('connection went away while sending data')
        finally:
            self._write_pending = False

    def purge(self):
        self.buf.seek(0)
        self.buf.truncate()
        self.bytes_read = self.bytes_written = 0

    def close(self):
        if self.is_closed:
            return False

        self.purge()
        self.buf.close()
        self.buf = None
        self.sendbuf.close()
        self.sendbuf = None

        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except:
            pass
        self._socket.close()
        self.is_closed = True
        return True


class ProtocolHandler(object):
    def handle(self, sock):
        first_byte = sock._read(1)
        if first_byte == b'$':
            return self.handle_string(sock)
        elif first_byte == b':':
            return self.handle_integer(sock)
        elif first_byte == b'*':
            return self.handle_array(sock)
        elif first_byte == b'%':
            return self.handle_dict(sock)
        elif first_byte == b'+':
            return self.handle_simple_string(sock)
        elif first_byte == b'-':
            return self.handle_error(sock)
        elif first_byte == b'~':
            return self.handle_float(sock)

        # Do no special handling and treat as a raw bytestring.
        rest = sock.readline()
        return first_byte + rest

    def handle_simple_string(self, sock):
        return sock.readline()

    def handle_error(self, sock):
        return Error(sock.readline())

    def handle_integer(self, sock):
        number = sock.readline()
        if b'.' in number:
            return float(number)
        return int(number)

    def handle_float(self, sock):
        val, = struct.unpack('>d', sock.read(8))
        return val

    def handle_string(self, sock):
        length = int(sock.readline())
        if length == -1:
            return None
        return sock.read(length)

    def handle_array(self, sock):
        num_elements = int(sock.readline())
        return [self.handle(sock) for _ in range(num_elements)]

    def handle_dict(self, sock):
        accum = {}
        num_items = int(sock.readline())
        for _ in range(num_items):
            key = self.handle(sock)
            accum[key] = self.handle(sock)
        return accum

    def write_response(self, sock, data, blocking=False):
        self._write(sock, data)
        sock.send(blocking)

    def _write(self, sock, data):
        if isinstance(data, unicode):
            data = encode(data)
        if isinstance(data, bytes):
            sock.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif data is True or data is False:
            sock.write(b':%d\r\n' % (1 if data else 0))
        elif isinstance(data, int_types):
            sock.write(b':%d\r\n' % data)
        elif data is None:
            sock.write(b'$-1\r\n')
        elif isinstance(data, Error):
            sock.write(b'-%s\r\n' % encode(data.message))
        elif isinstance(data, (list, tuple)):
            sock.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(sock, item)
        elif isinstance(data, dict):
            sock.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(sock, key)
                self._write(sock, data[key])
        elif isinstance(data, float):
            sock.write(b'~%s\r\n' % struct.pack('>d', data))
        else:
            raise ValueError('unrecognized type')


DEFAULT_MAP_SIZE = 1024 * 1024 * 256  # 256MB.


class Storage(object):
    def __init__(self, path, map_size=DEFAULT_MAP_SIZE, read_only=False,
                 metasync=True, sync=True, writemap=False, map_async=False,
                 meminit=True, max_dbs=16, max_spare_txns=64, lock=True,
                 dupsort=False):
        self._path = path
        self._config = {
            'map_size': DEFAULT_MAP_SIZE,
            'readonly': read_only,
            'metasync': metasync,
            'sync': sync,
            'writemap': writemap,
            'map_async': map_async,
            'meminit': meminit,
            'max_dbs': max_dbs,
            'max_spare_txns': max_spare_txns,
            'lock': lock,
        }
        self._dupsort = dupsort

        # Open LMDB environment and initialize data-structures.
        self.is_open = False
        self.open()

    def supports_dupsort(self, db):
        # Allow dupsort to be a list of db indexes or a simple boolean.
        if isinstance(self._dupsort, list):
            return db in self._dupsort
        return self._dupsort

    def open(self):
        if self.is_open:
            return False

        self.env = lmdb.open(self._path, **self._config)
        self.databases = {}

        for i in range(self._config['max_dbs']):
            # Allow databases to support duplicate values.
            self.databases[i] = self.env.open_db(
                encode('db%s' % i),
                dupsort=self.supports_dupsort(i))

        self.is_open = True
        return True

    def close(self):
        if not self.is_open:
            return False

        self.env.close()
        return True

    def reset(self):
        self.close()
        if os.path.exists(self._path):
            shutil.rmtree(self._path)
        return self.open()

    def stat(self):
        return self.env.stat()

    def count(self):
        return self.env.stat()['entries']

    def info(self):
        return self.env.info()

    @contextmanager
    def db(self, db=0, write=False):
        if not self.is_open:
            raise ValueError('Cannot operate on closed environment.')

        if not isinstance(db, int_types):
            raise ValueError('database index must be integer')

        txn = self.env.begin(db=self.databases[db], write=write)
        try:
            yield txn
        except Exception as exc:
            txn.abort()
            raise
        else:
            txn.commit()

    def flush(self, db):
        db_handle = self.databases[db]
        with self.db(db, True) as txn:
            return txn.drop(db_handle, delete=False)

    def flushall(self):
        return dict((db, self.flush(db)) for db in self.databases)

    def sync(self, force=False):
        return self.env.sync(force)


class Connection(object):
    def __init__(self, storage, sock):
        self.storage = storage
        self.sock = sock
        self.db = 0

    def use_db(self, idx):
        if idx not in self.storage.databases:
            raise CommandError('unrecognized database: %s' % idx)
        self.db = idx
        return self.db

    def ctx(self, write=False):
        return self.storage.db(self.db, write)

    @contextmanager
    def cursor(self, write=False):
        with self.ctx(write) as txn:
            cursor = txn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def close(self):
        self.sock.close()


mpackb = msgpack.packb
munpackb = msgpack.unpackb
def mpackdict(d):
    for key, value in d.items():
        yield (key, mpackb(value))


def requires_dupsort(meth):
    @wraps(meth)
    def verify_dupsort(self, client, *args):
        if not self.storage.supports_dupsort(client.db):
            raise CommandError('Currently-selected database %s does not '
                               'support dupsort.' % client.db)
        return meth(self, client, *args)
    return verify_dupsort


class Server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=1024,
                 path='data', **storage_config):
        self._host = host
        self._port = port
        self._max_clients = max_clients

        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (self._host, self._port),
            self.connection_handler,
            spawn=self._pool)

        self._commands = self.get_commands()
        self._protocol = ProtocolHandler()
        self.storage = Storage(path, **storage_config)

    def get_commands(self):
        accum = {}
        commands = (
            # Database / environment management.
            ('ENVINFO', self.env_info),
            ('ENVSTAT', self.env_stat),
            ('FLUSH', self.flush),
            ('FLUSHALL', self.flushall),
            ('STAT', self.stat),
            ('SYNC', self.sync),
            ('USEDB', self.use_db),

            # K/V operations.
            ('COUNT', self.count),
            ('DELETE', self.delete),
            ('DELETEDUP', self.deletedup),
            ('DUPCOUNT', self.dupcount),
            ('EXISTS', self.exists),
            ('GET', self.get),
            ('GETDUP', self.getdup),
            ('POP', self.pop),
            ('REPLACE', self.replace),
            ('SET', self.set),
            ('SETDUP', self.setdup),
            ('SETNX', self.setnx),

            # Bulk K/V operations.
            ('MDELETE', self.mdelete),
            ('MGET', self.mget),
            ('MGETDUP', self.mget),
            ('MGETDUP', self.mgetdup),
            ('MPOP', self.mpop),
            ('MREPLACE', self.mreplace),
            ('MSET', self.mset),
            ('MSETDUP', self.msetdup),
            ('MSETNX', self.msetnx),

            # Cursor operations.
            ('DELETERANGE', self.deleterange),
            ('GETRANGE', self.getrange),
            ('ITEMS', self.getrange),
            ('KEYS', self.keys),
            ('PREFIX', self.match_prefix),
            ('VALUES', self.values),

            # Client operations.
            ('SLEEP', self.client_sleep),
            ('QUIT', self.client_quit),
            ('SHUTDOWN', self.shutdown),
        )
        for cmd, callback in commands:
            accum[encode(cmd)] = callback
        return accum

    # Database / environment management.
    def env_info(self, client):
        return self.storage.info()

    def env_stat(self, client):
        return self.storage.stat()

    def flush(self, client):
        return self.storage.flush(client.db)

    def flushall(self, client):
        return self.storage.flushall()

    def stat(self, client):
        with client.ctx() as txn:
            return txn.stat()

    def sync(self, client):
        return self.storage.sync()

    def use_db(self, client, name):
        return client.use_db(name)

    # K/V operations.
    def count(self, client):
        with client.ctx() as txn:
            stat = txn.stat()
        return stat['entries']

    def delete(self, client, key):
        with client.ctx(True) as txn:
            return txn.delete(key)

    @requires_dupsort
    def deletedup(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.delete(key, mpackb(value))

    @requires_dupsort
    def dupcount(self, client, key):
        with client.cursor() as cursor:
            if not cursor.set_key(key):
                return
            return cursor.count()

    def exists(self, client, key):
        sentinel = object()
        with client.ctx() as txn:
            return txn.get(key, sentinel) is not sentinel

    def get(self, client, key):
        with client.ctx() as txn:
            res = txn.get(key)
            if res is not None:
                return munpackb(res)

    def getdup(self, client, key):
        with client.cursor() as cursor:
            if not cursor.set_key(key):
                return
            accum = []
            while cursor.key() == key:
                accum.append(munpackb(cursor.value()))
                if not cursor.next_dup():
                    break
        return accum

    def pop(self, client, key):
        with client.ctx(True) as txn:
            res = txn.pop(key)
            if res is not None:
                return munpackb(res)

    def replace(self, client, key, value):
        with client.ctx(True) as txn:
            old_val = txn.replace(key, mpackb(value))
            if old_val is not None:
                return munpackb(old_val)

    def set(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.put(key, mpackb(value), dupdata=False, overwrite=True)

    @requires_dupsort
    def setdup(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.put(key, mpackb(value), dupdata=True, overwrite=True)

    def setnx(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.put(key, mpackb(value), dupdata=False, overwrite=False)

    # Bulk K/V operations.
    def mdelete(self, client, keys):
        n = 0
        with client.ctx(True) as txn:
            for key in keys:
                if txn.delete(key):
                    n += 1
        return n

    def mget(self, client, keys):
        accum = {}
        with client.ctx() as txn:
            for key in keys:
                res = txn.get(key)
                if res is not None:
                    accum[key] = munpackb(res)
        return accum

    def mgetdup(self, client, keys):
        accum = {}
        with client.cursor() as cursor:
            for key in keys:
                if cursor.set_key(key):
                    values = []
                    while cursor.key() == key:
                        values.append(munpackb(cursor.value()))
                        if not cursor.next_dup():
                            break
                    accum[key] = values
        return accum

    def mpop(self, client, keys):
        accum = {}
        with client.cursor(True) as cursor:
            for key in keys:
                res = cursor.pop(key)
                if res is not None:
                    accum[key] = munpackb(res)
        return accum

    def mreplace(self, client, data):
        accum = {}
        with client.cursor(True) as cursor:
            for key, value in data.items():
                old_val = cursor.replace(key, mpackb(value))
                if old_val is not None:
                    accum[key] = munpackb(old_val)
        return accum

    def mset(self, client, data):
        with client.cursor(True) as cursor:
            consumed, added = cursor.putmulti(mpackdict(data), dupdata=False)
        return added

    def msetdup(self, client, data):
        with client.cursor(True) as cursor:
            consumed, added = cursor.putmulti(mpackdict(data))
        return added

    def msetnx(self, client, data):
        with client.cursor(True) as cursor:
            consumed, added = cursor.putmulti(mpackdict(data), dupdata=False,
                                              overwrite=False)
        return added

    # Cursor operations.
    def deleterange(self, client, start=None, stop=None, count=None):
        if count is None:
            count = 0
        n = 0

        with client.cursor(write=True) as cursor:
            if start is None:
                if not cursor.first():
                    return n
            elif not cursor.set_range(start):
                return n

            while True:
                key = cursor.key()
                if stop is not None and key > stop:
                    break

                if not cursor.delete():
                    break

                n += 1
                count -= 1
                if count == 0:
                    break

        return n

    def _cursor_op(self, client, start, stop, count, cb, stopcond=operator.gt):
        accum = []
        if count is None:
            count = 0

        with client.cursor() as cursor:
            if start is None:
                if not cursor.first():
                    return []
            elif not cursor.set_range(start):
                return []

            while True:
                key, data = cb(cursor)
                if stop is not None and stopcond(key, stop):
                    break
                accum.append(data)
                count -= 1
                if count == 0 or not cursor.next():
                    break

        return accum

    def getrange(self, client, start=None, stop=None, count=None):
        def cb(cursor):
            key, value = cursor.item()
            return key, (key, munpackb(value))
        return self._cursor_op(client, start, stop, count, cb)

    def keys(self, client, start=None, stop=None, count=None):
        def cb(cursor):
            key = cursor.key()
            return key, key
        return self._cursor_op(client, start, stop, count, cb)

    def values(self, client, start=None, stop=None, count=None):
        def cb(cursor):
            key, value = cursor.item()
            return key, munpackb(value)
        return self._cursor_op(client, start, stop, count, cb)

    def match_prefix(self, client, prefix, count=None):
        def cb(cursor):
            key, value = cursor.item()
            return key, (key, munpackb(value))
        stopcond = lambda k, p: not k.startswith(p)
        return self._cursor_op(client, prefix, prefix, count, cb, stopcond)

    # Client operations.
    def client_sleep(self, client, timeout=1):
        gevent.sleep(timeout)
        return timeout

    def client_quit(self, client):
        raise ClientQuit('client closed connection')

    def shutdown(self, client):
        raise Shutdown('shutting down')

    # Server implementation.
    def run(self):
        try:
            self._server.serve_forever()
        finally:
            self.storage.close()

    def connection_handler(self, conn, address):
        logger.info('Connection received: %s:%s' % address)
        client = Connection(self.storage, _Socket(conn))
        while True:
            try:
                self.request_response(client)
            except ConnectionError:
                logger.info('Client went away: %s:%s' % address)
                client.close()
                break
            except ClientQuit:
                logger.info('Client exited: %s:%s.' % address)
                break
            except Exception as exc:
                logger.exception('Error processing command.')

    def request_response(self, client):
        data = self._protocol.handle(client.sock)

        try:
            resp = self.respond(client, data)
        except Shutdown:
            logger.info('Shutting down')
            self._protocol.write_response(client.sock, 1, True)
            raise KeyboardInterrupt
        except ClientQuit:
            self._protocol.write_response(client.sock, 1, True)
            raise
        except CommandError as command_error:
            resp = Error(command_error.message)
        except Exception as exc:
            logger.exception('Unhandled error')
            resp = Error('Unhandled server error: "%s"' % str(exc))

        self._protocol.write_response(client.sock, resp)

    def respond(self, client, data):
        if not isinstance(data, (list, tuple)):
            try:
                data = data.split()
            except:
                raise CommandError('Unrecognized request type.')

        if not isinstance(data[0], basestring):
            raise CommandError('First parameter must be command name.')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        else:
            logger.debug('Received %s', decode(command))

        return self._commands[command](client, *data[1:])


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        self.host = host
        self.port = port
        self._protocol = ProtocolHandler()
        self._sock = None
        self.connect()

    def connect(self):
        if self._sock is not None:
            self._sock.close()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.connect((self.host, self.port))
        self._sock = _Socket(sock)
        return True

    def close(self):
        if self._sock is None:
            return False

        self._sock.close()
        self._sock = None
        return True

    def read_response(self, close_conn=False):
        try:
            resp = self._protocol.handle(self._sock)
        except EOFError:
            self._sock.close()
            raise ConnectionError('server went away')
        except Exception:
            self._sock.close()
            raise ServerInternalError('internal server error')
        else:
            if close_conn:
                self._sock.close()
        if isinstance(resp, Error):
            raise CommandError(decode(resp.message))
        return resp

    def execute(self, *args):
        if self._sock is None:
            raise ConnectionError('not connected!')

        close_conn = args[0] in (b'QUIT', b'SHUTDOWN')
        self._protocol.write_response(self._sock, args)
        return self.read_response(close_conn)

    def command(cmd):
        def method(self, *args):
            return self.execute(encode(cmd), *args)
        return method

    # Database/schema management.
    env_info = command('ENVINFO')
    env_stat = command('ENVSTAT')
    flush = command('FLUSH')
    flushall = command('FLUSHALL')
    stat = command('STAT')
    sync = command('SYNC')
    use = command('USEDB')

    # Basic k/v operations.
    count = command('COUNT')
    delete = command('DELETE')
    deletedup = command('DELETEDUP')
    dupcount = command('DUPCOUNT')
    exists = command('EXISTS')
    get = command('GET')
    getdup = command('GETDUP')
    pop = command('POP')
    replace = command('REPLACE')
    set = command('SET')
    setdup = command('SETDUP')
    setnx = command('SETNX')

    # Bulk k/v operations.
    mdelete = command('MDELETE')
    mget = command('MGET')
    mgetdup = command('MGETDUP')
    mpop = command('MPOP')
    mreplace = command('MREPLACE')
    mset = command('MSET')
    msetdup = command('MSETDUP')
    msetnx = command('MSETNX')

    # Cursor operations.
    deleterange = command('DELETERANGE')
    getrange = command('GETRANGE')
    items = command('ITEMS')
    keys = command('KEYS')
    prefix = command('PREFIX')
    values = command('VALUES')

    # Client operations.
    _sleep = command('SLEEP')
    quit = command('QUIT')
    shutdown = command('SHUTDOWN')


def get_option_parser():
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', default='config.json', dest='config',
                      help='Config file (default="config.json")')
    parser.add_option('-D', '--data-dir', default='data', dest='data_dir',
                      help='Directory to store db environment and data.')
    parser.add_option('-d', '--debug', action='store_true', dest='debug',
                      help='Log debug messages.')
    parser.add_option('-e', '--errors', action='store_true', dest='error',
                      help='Log error messages only.')
    parser.add_option('-H', '--host', default='127.0.0.1', dest='host',
                      help='Host to listen on.')
    parser.add_option('-l', '--log-file', dest='log_file', help='Log file.')
    parser.add_option('-m', '--max-clients', default=1024, dest='max_clients',
                      help='Maximum number of clients.', type=int)
    parser.add_option('-p', '--port', default=31337, dest='port',
                      help='Port to listen on.', type=int)
    parser.add_option('-r', '--reset', action='store_true', dest='reset',
                      help='Reset database and config. All data will be lost.')
    return parser


def configure_logger(options):
    logger.addHandler(logging.StreamHandler())
    if options.log_file:
        logger.addHandler(logging.FileHandler(options.log_file))
    if options.debug:
        logger.setLevel(logging.DEBUG)
    elif options.error:
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.INFO)


def read_config(config_file):
    if os.path.exists(config_file):
        with open(config_file) as fh:
            return json.loads(fh.read())
    return {}


if __name__ == '__main__':
    options, args = get_option_parser().parse_args()

    configure_logger(options)
    if options.reset and os.path.exists(options.data_dir):
        shutil.rmtree(options.data_dir)

    config = read_config(options.config or 'config.json')
    config.setdefault('host', options.host)
    config.setdefault('port', options.port)
    config.setdefault('max_clients', options.max_clients)
    config.setdefault('path', options.data_dir)
    server = Server(**config)

    print('\x1b[32m  .--.')
    print(' /( \x1b[34m@\x1b[33m >\x1b[32m    ,-.  '
          '\x1b[1;32mgreendb '
          '\x1b[1;33m%s:%s\x1b[32m' % (server._host, server._port))
    print('/ \' .\'--._/  /')
    print(':   ,    , .\'')
    print('\'. (___.\'_/')
    print(' \x1b[33m((\x1b[32m-\x1b[33m((\x1b[32m-\'\'\x1b[0m')
    try:
        server.run()
    except KeyboardInterrupt:
        print('\x1b[1;31mshutting down\x1b[0m')

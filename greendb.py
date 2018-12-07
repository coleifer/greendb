#!/usr/bin/env python

from __future__ import unicode_literals  # Required for 2.x compatability.

import gevent
from gevent import socket
from gevent.local import local as greenlet_local
from gevent.pool import Pool
from gevent.server import StreamServer
from gevent.thread import get_ident

import lmdb
from msgpack import packb as msgpack_packb
from msgpack import unpackb as msgpack_unpackb

from collections import namedtuple
from contextlib import contextmanager
from functools import wraps
from io import BytesIO
from socket import error as socket_error
import datetime
import heapq
import json
import logging
import operator
import optparse
import os
import re
import shutil
import sys
import struct
import time


__version__ = '0.2.0'

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

def encode_bulk_dict(d):
    accum = {}
    for key, value in d.items():
        accum[encode(key)] = value
    return accum

def encode_bulk_list(l):
    return [encode(k) for k in l]

def decode(s):
    if isinstance(s, unicode):
        return s
    elif isinstance(s, bytes):
        return s.decode('utf-8')
    else:
        return str(s)

def decode_bulk_dict(d):
    accum = {}
    for key, value in d.items():
        accum[decode(key)] = value
    return accum


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
ProcessingInstruction = namedtuple('ProcessingInstruction', ('op', 'value'))

PI_USE_DB = b'\x01'
PROCESSING_INSTRUCTIONS = set((PI_USE_DB,))

CRLF = b'\r\n'
READSIZE = 4 * 1024


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
        if self.is_closed:
            return

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
        elif first_byte == b'^':
            return self.handle_unicode(sock)
        elif first_byte == b':':
            return self.handle_integer(sock)
        elif first_byte == b'.':
            return self.handle_processing_instruction(sock)
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

    def handle_processing_instruction(self, sock):
        instruction = sock.read(1)
        if instruction not in PROCESSING_INSTRUCTIONS:
            raise ValueError('unrecognized processing instruction, refusing to'
                             ' process.')
        return ProcessingInstruction(instruction, self.handle(sock))

    def handle_string(self, sock):
        length = int(sock.readline())
        if length >= 0:
            return sock.read(length)

    def handle_unicode(self, sock):
        length = int(sock.readline())
        if length >= 0:
            return sock.read(length).decode('utf-8')

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
        if isinstance(data, bytes):
            sock.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, unicode):
            data = encode(data)
            sock.write(b'^%d\r\n%s\r\n' % (len(data), data))
        elif data is True or data is False:
            sock.write(b':%d\r\n' % (1 if data else 0))
        elif isinstance(data, int_types):
            sock.write(b':%d\r\n' % data)
        elif data is None:
            sock.write(b'$-1\r\n')
        elif isinstance(data, Error):
            sock.write(b'-%s\r\n' % encode(data.message))
        elif isinstance(data, ProcessingInstruction):
            sock.write(b'.%s\r\n' % encode(data.op))
            self._write(sock, data.value)
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
                 metasync=True, sync=False, writemap=False, map_async=False,
                 meminit=True, max_dbs=16, max_spare_txns=64, lock=True,
                 dupsort=False):
        self._path = path
        self._config = {
            'map_size': map_size,
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

        self.sync(True)  # Always sync before closing.
        self.env.close()
        return True

    def reset(self):
        self.close()
        if os.path.exists(self._path):
            shutil.rmtree(self._path)
        return self.open()

    def get_storage_config(self):
        config = {'path': self._path, 'dupsort': self._dupsort}
        config.update(self._config)  # Add LMDB config.
        return config

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
            txn.drop(db_handle, delete=False)
        return True

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
        if not isinstance(idx, int):
            raise CommandError('database index must be an integer')
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


mpackb = lambda o: msgpack_packb(o, use_bin_type=True)
munpackb = lambda b: msgpack_unpackb(b, raw=False)
def mpackdict(d):
    for key, value in d.items():
        yield (encode(key), mpackb(value))


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
            ('ENVINFO', self.envinfo),
            ('ENVSTAT', self.envstat),
            ('FLUSH', self.flush),
            ('FLUSHALL', self.flushall),
            ('PING', self.ping),
            ('STAT', self.stat),
            ('SYNC', self.sync),
            ('USE', self.use_db),

            # K/V operations.
            ('COUNT', self.count),
            ('DECR', self.decr),
            ('INCR', self.incr),
            ('CAS', self.cas),
            ('DELETE', self.delete),
            ('DELETEDUP', self.deletedup),
            ('DELETEDUPRAW', self.deletedupraw),
            ('DUPCOUNT', self.dupcount),
            ('EXISTS', self.exists),
            ('GET', self.get),
            ('GETDUP', self.getdup),
            ('LENGTH', self.length),
            ('POP', self.pop),
            ('REPLACE', self.replace),
            ('SET', self.set),
            ('SETDUP', self.setdup),
            ('SETDUPRAW', self.setdupraw),
            ('SETNX', self.setnx),

            # Bulk K/V operations.
            ('MDELETE', self.mdelete),
            ('MGET', self.mget),
            ('MGETDUP', self.mgetdup),
            ('MPOP', self.mpop),
            ('MREPLACE', self.mreplace),
            ('MSET', self.mset),
            ('MSETDUP', self.msetdup),
            ('MSETNX', self.msetnx),

            # Cursor operations.
            ('DELETERANGE', self.deleterange),
            ('GETRANGE', self.getrange),
            ('GETRANGEDUPRAW', self.getrangedupraw),
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
    def envinfo(self, client):
        info = self.storage.info()
        info.update(
            clients=len(self._pool),
            host=self._host,
            port=self._port,
            max_clients=self._max_clients,
            storage=self.storage.get_storage_config())
        return info

    def envstat(self, client):
        return self.storage.stat()

    def flush(self, client):
        return self.storage.flush(client.db)

    def flushall(self, client):
        return self.storage.flushall()

    def ping(self, client):
        return b'pong'

    def stat(self, client):
        with client.ctx() as txn:
            return txn.stat()

    def sync(self, client):
        return self.storage.sync()

    def use_db(self, client, idx):
        return client.use_db(idx)

    # K/V operations.
    def count(self, client):
        with client.ctx() as txn:
            stat = txn.stat()
        return stat['entries']

    def decr(self, client, key, amount=1):
        return self._incr(client, encode(key), -amount)

    def incr(self, client, key, amount=1):
        return self._incr(client, encode(key), amount)

    def _incr(self, client, key, amount):
        with client.cursor(True) as cursor:
            # If the key does not exist, just set the desired value.
            if not cursor.set_key(key):
                cursor.put(key, mpackb(amount))
                return amount

            orig = munpackb(cursor.value())
            try:
                value = orig + amount
            except TypeError:
                raise CommandError('decr operation on wrong type of value')

            cursor.delete()
            cursor.put(key, mpackb(value))
            return value

    def cas(self, client, key, old_value, new_value):
        key = encode(key)
        with client.ctx(True) as txn:
            value = txn.get(key)
            if value is not None and munpackb(value) == old_value:
                if self.storage.supports_dupsort(client.db):
                    txn.delete(key, value)
                txn.put(key, mpackb(new_value))
                return True
            elif value is None and old_value is None:
                txn.put(key, mpackb(new_value))
                return True
            else:
                return False

    def delete(self, client, key):
        with client.ctx(True) as txn:
            return txn.delete(encode(key))

    @requires_dupsort
    def deletedup(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.delete(encode(key), mpackb(value))

    @requires_dupsort
    def deletedupraw(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.delete(encode(key), encode(value))

    @requires_dupsort
    def dupcount(self, client, key):
        with client.cursor() as cursor:
            if not cursor.set_key(encode(key)):
                return
            return cursor.count()

    def exists(self, client, key):
        sentinel = object()
        with client.ctx() as txn:
            return txn.get(encode(key), sentinel) is not sentinel

    def get(self, client, key):
        with client.ctx() as txn:
            res = txn.get(encode(key))
            if res is not None:
                return munpackb(res)

    def getdup(self, client, key):
        key = encode(key)
        with client.cursor() as cursor:
            if not cursor.set_key(key):
                return
            accum = []
            while cursor.key() == key:
                accum.append(munpackb(cursor.value()))
                if not cursor.next_dup():
                    break
        return accum

    def length(self, client, key):
        value = self.get(client, key)
        if value is not None:
            try:
                return len(value)
            except TypeError:
                raise CommandError('incompatible type for LENGTH command')

    def pop(self, client, key):
        with client.ctx(True) as txn:
            res = txn.pop(encode(key))
            if res is not None:
                return munpackb(res)

    def replace(self, client, key, value):
        with client.ctx(True) as txn:
            old_val = txn.replace(encode(key), mpackb(value))
            if old_val is not None:
                return munpackb(old_val)

    def set(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.put(encode(key), mpackb(value), dupdata=False)

    @requires_dupsort
    def setdup(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.put(encode(key), mpackb(value))

    @requires_dupsort
    def setdupraw(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.put(encode(key), encode(value))

    def setnx(self, client, key, value):
        with client.ctx(True) as txn:
            return txn.put(encode(key), mpackb(value), dupdata=False,
                           overwrite=False)

    # Bulk K/V operations.
    def mdelete(self, client, keys):
        n = 0
        with client.ctx(True) as txn:
            for key in map(encode, keys):
                if txn.delete(key):
                    n += 1
        return n

    def mget(self, client, keys):
        accum = {}
        with client.ctx() as txn:
            for key in map(encode, keys):
                res = txn.get(key)
                if res is not None:
                    accum[key] = munpackb(res)
        return accum

    def mgetdup(self, client, keys):
        accum = {}
        with client.cursor() as cursor:
            for key in map(encode, keys):
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
            for key in map(encode, keys):
                res = cursor.pop(key)
                if res is not None:
                    accum[key] = munpackb(res)
        return accum

    def mreplace(self, client, data):
        accum = {}
        with client.cursor(True) as cursor:
            for key, value in data.items():
                key = encode(key)
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
        stop = encode(stop) if stop is not None else stop
        n = 0

        with client.cursor(write=True) as cursor:
            if start is None:
                if not cursor.first():
                    return n
            elif not cursor.set_range(encode(start)):
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
        stop = encode(stop) if stop is not None else stop

        with client.cursor() as cursor:
            if start is None:
                if not cursor.first():
                    return []
            elif not cursor.set_range(encode(start)):
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

    @requires_dupsort
    def getrangedupraw(self, client, key, start=None, stop=None, count=None):
        accum = []
        if count is None:
            count = 0
        stop = encode(stop) if stop is not None else stop

        with client.cursor() as cursor:
            if start is None:
                if not cursor.set_range(encode(key)):
                    return []
            elif not cursor.set_range_dup(encode(key), encode(start)):
                return []

            while True:
                value = cursor.value()
                if stop is not None and value > stop:
                    break
                accum.append(value)
                count -= 1
                if count == 0 or not cursor.next_dup():
                    break

        return accum

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

        # If we received a processing instruction, it will be handled here, as
        # the next request will contain the relevant data.
        if isinstance(data, ProcessingInstruction):
            self.execute_processing_instruction(client, data)
            return

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

    def execute_processing_instruction(self, client, pi):
        if pi.op == PI_USE_DB and client.db != pi.value:
            orig_db = client.db
            try:
                client.use_db(pi.value)
                self.request_response(client)
            finally:
                client.use_db(orig_db)

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


class _ConnectionState(object):
    def __init__(self, **kwargs):
        super(_ConnectionState, self).__init__(**kwargs)
        self.reset()
    def reset(self): self.conn = None
    def set_connection(self, conn): self.conn = conn

class _ConnectionLocal(_ConnectionState, greenlet_local): pass


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337, decode_keys=False,
                 timeout=60):
        self.host = host
        self.port = port
        self._decode_keys = decode_keys
        self._timeout = timeout
        self._protocol = ProtocolHandler()
        self._state = _ConnectionLocal()

    def is_closed(self):
        return self._state.conn is None

    def close(self):
        if self._state.conn is None: return False
        self._state.conn.close()
        self._state.reset()
        return True

    def connect(self):
        if self._state.conn is not None: return False
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(self._timeout)
        sock.connect((self.host, self.port))
        self._state.conn = _Socket(sock)
        return True

    def read_response(self, conn, close_conn=False):
        try:
            resp = self._protocol.handle(conn)
        except EOFError:
            self.close()
            raise ConnectionError('server went away')
        except Exception:
            self.close()
            raise ServerInternalError('internal server error')
        else:
            if close_conn:
                self.close()
        if isinstance(resp, Error):
            raise CommandError(decode(resp.message))
        if self._decode_keys and isinstance(resp, dict):
            resp = decode_bulk_dict(resp)
        return resp

    def execute(self, cmd, args, close_conn=False, db=None):
        if self._state.conn is None:
            self.connect()

        conn = self._state.conn

        # If the db is explicitly specified for a one-off command, then
        # handle this using a processing instruction.
        if db is not None:
            pi = ProcessingInstruction(PI_USE_DB, db)
            self._protocol._write(conn, pi)

        # Execute the command. The command and its arguments (if it has any)
        # are all packed into a tuple and written to the server as a list.
        self._protocol.write_response(conn, (cmd,) + args)
        return self.read_response(conn, close_conn)

    def command(cmd, close_conn=False):
        def method(self, *args, **kwargs):
            return self.execute(encode(cmd), args, close_conn, **kwargs)
        return method

    # Database/schema management.
    envinfo = command('ENVINFO')
    envstat = command('ENVSTAT')
    flush = command('FLUSH')
    flushall = command('FLUSHALL')
    ping = command('PING')
    stat = command('STAT')
    sync = command('SYNC')
    use = command('USE')

    # Basic k/v operations.
    count = command('COUNT')
    decr = command('DECR')
    incr = command('INCR')
    cas = command('CAS')
    delete = command('DELETE')
    deletedup = command('DELETEDUP')
    deletedupraw = command('DELETEDUPRAW')
    dupcount = command('DUPCOUNT')
    exists = command('EXISTS')
    get = command('GET')
    getdup = command('GETDUP')
    length = command('LENGTH')
    pop = command('POP')
    replace = command('REPLACE')
    set = command('SET')
    setdup = command('SETDUP')
    setdupraw = command('SETDUPRAW')
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
    getrangedupraw = command('GETRANGEDUPRAW')
    items = command('ITEMS')
    keys = command('KEYS')
    prefix = command('PREFIX')
    values = command('VALUES')

    # Client operations.
    _sleep = command('SLEEP')
    quit = command('QUIT', close_conn=True)
    shutdown = command('SHUTDOWN', close_conn=True)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.getrange(item.start, item.stop, item.step)
        elif isinstance(item, (list, tuple)):
            return self.mget(item)
        return self.get(item)

    def __delitem__(self, item):
        if isinstance(item, slice):
            self.deleterange(item.start, item.stop, item.step)
        elif isinstance(item, (list, tuple)):
            self.mdelete(item)
        else:
            self.delete(item)

    __len__ = count
    __contains__ = exists

    def update(self, __data=None, **kwargs):
        if __data is not None:
            params = __data
            params.update(kwargs)
        else:
            params = kwargs
        return self.mset(params)

    def __iter__(self):
        return iter(self.keys())


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
    parser.add_option('-m', '--map-size', dest='map_size', help=(
        'Maximum size of memory-map used for database. The default value is '
        '256M and should be increased. Accepts value in bytes or file-size '
        'using "M" or "G" suffix.'))
    parser.add_option('--max-clients', default=1024, dest='max_clients',
                      help='Maximum number of clients.', type=int)
    parser.add_option('-n', '--max-dbs', default=16, dest='max_dbs',
                      help='Number of databases in environment. Default=16.',
                      type='int')
    parser.add_option('-p', '--port', default=31337, dest='port',
                      help='Port to listen on.', type=int)
    parser.add_option('-r', '--reset', action='store_true', dest='reset',
                      help='Reset database and config. All data will be lost.')
    parser.add_option('-s', '--sync', action='store_true', dest='sync',
                      help=('Flush system buffers to disk when committing a '
                            'transaction. Durable but much slower.'))
    parser.add_option('-u', '--dupsort', action='append', dest='dupsort',
                      help='db index(es) to support dupsort', type='int'),
    parser.add_option('-M', '--no-metasync', action='store_true',
                      dest='no_metasync', help=(
                          'Flush system buffers to disk only once per '
                          'transaction, omit the metadata flush.'))
    parser.add_option('-W', '--writemap', action='store_true', dest='writemap',
                      help='Use a writeable memory map.')
    parser.add_option('-A', '--map-async', action='store_true',
                      dest='map_async', help=(
                          'When used with "--writemap" (-W), use asynchronous '
                          'flushes to disk.'))
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
    if not os.path.exists(config_file): return {}

    with open(config_file) as fh:
        data = fh.read()

    # Strip comments.
    config = json.loads(re.sub('\s*\/\/.*', '', data))

    if config.get('map_size'):
        config['map_size'] = parse_map_size(config['map_size'])
    return config

def log_config(conf):
    for key, value in sorted(conf.items()):
        if value is not None:
            logger.debug('%s=%s' % (key, value))

def parse_map_size(value):
    mapsize = value.lower() if value else '256m'

    if mapsize.endswith('b'):
        mapsize = mapsize[:-1]  # Strip "b", as in "mb", "gb", etc.
    n = 1
    if mapsize.endswith('k'):
        exp = 1024
    elif mapsize.endswith('m'):
        exp = 1024 * 1024
    elif mapsize.endswith('g'):
        exp = 1024 * 1024 * 1024
    else:
        exp = 1
        n = 0
    mapsize = mapsize[:-n] if n else mapsize  # Strip trailing letter.
    if not mapsize.isdigit():
        raise ValueError('cannot parse file-size "%s", use a valid file-'
                         'size like "256m" or "1g"' % value)
    return int(mapsize) * exp


if __name__ == '__main__':
    options, args = get_option_parser().parse_args()

    configure_logger(options)
    if options.reset and os.path.exists(options.data_dir):
        shutil.rmtree(options.data_dir)

    config = read_config(options.config or 'config.json')
    config.setdefault('path', options.data_dir)
    config.setdefault('host', options.host)
    config.setdefault('map_size', parse_map_size(options.map_size))
    config.setdefault('max_clients', options.max_clients)
    config.setdefault('max_dbs', options.max_dbs)
    config.setdefault('port', options.port)
    config.setdefault('sync', bool(options.sync))
    config.setdefault('dupsort', options.dupsort)
    config.setdefault('metasync', not options.no_metasync)
    config.setdefault('writemap', options.writemap)
    config.setdefault('map_async', options.map_async)
    server = Server(**config)

    print('\x1b[32m  .--.')
    print(' /( \x1b[34m@\x1b[33m >\x1b[32m    ,-.  '
          '\x1b[1;32mgreendb '
          '\x1b[1;33m%s:%s\x1b[32m' % (server._host, server._port))
    print('/ \' .\'--._/  /')
    print(':   ,    , .\'')
    print('\'. (___.\'_/')
    print(' \x1b[33m((\x1b[32m-\x1b[33m((\x1b[32m-\'\'\x1b[0m')
    log_config(config)
    try:
        server.run()
    except KeyboardInterrupt:
        print('\x1b[1;31mshutting down\x1b[0m')

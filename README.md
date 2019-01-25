![](http://media.charlesleifer.com/blog/photos/logo-0.png)

### greendb

async server frontend for symas lmdb.

#### description

greendb is a lightweight server (and Python client) for symas lmdb. The server
uses the new Redis RESPv3 protocol.

greendb supports multiple independent databases, much like Redis. Values are
serialized using `msgpack`, so the server is capable of storing all the
data-types supported by msgpack. greendb also provides per-database
configuration of multi-value support, allowing you to efficiently store
multiple values at a given key, in sorted order (e.g. for secondary indexes).

With greendb, database keys are always bytestrings. Values may be:

* dict
* list
* set
* bytestrings
* unicode strings
* integers
* floating-point
* boolean
* `None`

#### installing

```
$ pip install greendb
```

Alternatively, you can install from git:

```
$ git clone https://github.com/coleifer/greendb
$ cd greendb
$ python setup.py install
```

Dependencies:

* [gevent](http://www.gevent.org/)
* [lmdb](https://github.com/dw/py-lmdb)
* [msgpack-python](https://github.com/msgpack/msgpack-python)

#### running

```
$ greendb.py -h

Usage: greendb.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIG, --config=CONFIG
                        Config file (default="config.json")
  -D DATA_DIR, --data-dir=DATA_DIR
                        Directory to store db environment and data.
  -d, --debug           Log debug messages.
  -e, --errors          Log error messages only.
  -H HOST, --host=HOST  Host to listen on.
  -l LOG_FILE, --log-file=LOG_FILE
                        Log file.
  -m MAP_SIZE, --map-size=MAP_SIZE
                        Maximum size of memory-map used for database. The
                        default value is 256M and should be increased. Accepts
                        value in bytes or file-size using "M" or "G" suffix.
  --max-clients=MAX_CLIENTS
                        Maximum number of clients.
  -n MAX_DBS, --max-dbs=MAX_DBS
                        Number of databases in environment. Default=16.
  -p PORT, --port=PORT  Port to listen on.
  -r, --reset           Reset database and config. All data will be lost.
  -s, --sync            Flush system buffers to disk when committing a
                        transaction. Durable but much slower.
  -u DUPSORT, --dupsort=DUPSORT
                        db index(es) to support dupsort
  -M, --no-metasync     Flush system buffers to disk only once per
                        transaction, omit the metadata flush.
  -W, --writemap        Use a writeable memory map.
  -A, --map-async       When used with "--writemap" (-W), use asynchronous
                        flushes to disk.
```

Complete config file example with default values:

```javascript

{
  "host": "127.0.0.1",
  "port": 31337,
  "max_clients": 1024,
  "path": "data",  // Directory for data storage, default is "data" in CWD.
  "map_size": "256M",  // Default map size is 256MB. INCREASE THIS!
  "read_only": false,  // Open the database in read-only mode.
  "metasync": true,  // Sync metadata changes (recommended).
  "sync": false,  // Sync all changes (durable, but much slower).
  "writemap": false,  // Use a writable map (probably safe to do).
  "map_async": false,  // Asynchronous writable map.
  "meminit": true,  // Initialize new memory pages to zero.
  "max_dbs": 16,  // Maximum number of DBs.
  "max_spare_txns": 64,
  "lock": true,  // Lock the database when opening environment.
  "dupsort": false  // Either a boolean or a list of DB indexes.
}
```

Example custom configuration:

* 1GB max database size
* dupsort enabled on databases 13, 14 and 15
* data stored in /var/lib/greendb/data

```javascript
{
  "map_size": "1G",
  "dupsort": [13, 14, 15],
  "path": "/var/lib/greendb/data/"
}
```

Equivalent configuration using command-line arguments:

```
$ greendb.py -m 1G -u 13 -u 14 -u 15 -D /var/lib/greendb/data/
```

### client

A Python client is included in the `greendb` module. All server commands are
implemented as client methods using the lower-case command name, for example:

```python

from greendb import Client
client = Client(host='10.0.0.3')

# Execute the ENVINFO command.
print(client.envinfo())

# Set multiple key/value pairs, read a key, then delete two keys.
client.mset({'k1': 'v1', 'k2': 'v2'})  # MSET
print(client.get('k1'))  # GET
client.mdelete(['k1', 'k2'])  # MDELETE
```

Additionally, the `Client` implements much of the Python `dict` interface, such
as item get/set/delete, iteration, length, contains, etc.

If an error occurs, either due to a malformed command (e.g., missing required
parameters) or for any other reason (e.g., attempting to write to a read-only
database), then a `CommandError` will be raised by the client with a message
indicating what caused the error.

**A note about connections**: the greendb client will automatically connect the
first time you issue a command to the server. The client maintains its own
thread-safe (and greenlet-safe) connection pool. If you wish to explicitly
connect, the client may be used as a context manager.

In a multi-threaded or multi-greenlet application (e.g. a web app), the client
will maintain a separate connection for each thread/greenlet.

### command reference

Below is the list of supported commands. **Commands are available on the client
using the lower-case command name as the method name**.

<table>
  <thead>
    <th>command</th>
    <th>description</th>
    <th>arguments</th>
    <th>return value</th>
  </thead>
  <tr>
    <td>ENVINFO</td>
    <td>metadata and storage configuration settings</td>
    <td>(none)</td>
    <td>dict</td>
  </tr>
  <tr>
    <td>ENVSTAT</td>
    <td>metadata related to the global b-tree</td>
    <td>(none)</td>
    <td>dict</td>
  </tr>
  <tr>
    <td>FLUSH</td>
    <td>delete all records in the currently-selected database</td>
    <td>(none)</td>
    <td>boolean indicating success</td>
  </tr>
  <tr>
    <td>FLUSHALL</td>
    <td>delete all records in all databases</td>
    <td>(none)</td>
    <td>dict mapping database index to boolean</td>
  </tr>
  <tr>
    <td>PING</td>
    <td>ping the server</td>
    <td>(none)</td>
    <td>"pong"</td>
  </tr>
  <tr>
    <td>STAT</td>
    <td>metadata related to the currently-selected database b-tree</td>
    <td>(none)</td>
    <td>dict</td>
  </tr>
  <tr>
    <td>SYNC</td>
    <td>synchronize database to disk (use when sync=False)</td>
    <td>(none)</td>
    <td>(none)</td>
  </tr>
  <tr>
    <td>USE</td>
    <td>select the given database</td>
    <td>database index, 0 through (max_dbs - 1)</td>
    <td>int: active database index</td>
  </tr>
  <tr>
    <th>KV commands</th>
  </tr>
  <tr>
    <td>COUNT</td>
    <td>get the number of key/value pairs in active database</td>
    <td>(none)</td>
    <td>int</td>
  </tr>
  <tr>
    <td>DECR</td>
    <td>decrement the value at the given key</td>
    <td>amount to decrement by (optional, default is 1)</td>
    <td>int or float</td>
  </tr>
  <tr>
    <td>INCR</td>
    <td>increment the value at the given key</td>
    <td>amount to increment by (optional, default is 1)</td>
    <td>int or float</td>
  </tr>
  <tr>
    <td>CAS</td>
    <td>compare-and-set</td>
    <td>key, original value, new value</td>
    <td>boolean indicating success or failure</td>
  </tr>
  <tr>
    <td>DELETE</td>
    <td>delete a key and any value(s) associated</td>
    <td>key</td>
    <td>int: number of keys removed (1 on success, 0 if key not found)</td>
  </tr>
  <tr>
    <td>DELETEDUP</td>
    <td>delete a particular key/value pair when dupsort is enabled</td>
    <td>key, value to delete</td>
    <td>int: number of key+value removed (1 on success, 0 if key+value not found)</td>
  </tr>
  <tr>
    <td>DELETEDUPRAW</td>
    <td>delete a particular key/value pair when dupsort is enabled using an
    unserialized bytestring as the value</td>
    <td>key, value to delete</td>
    <td>int: number of key+value removed (1 on success, 0 if key+value not found)</td>
  </tr>
  <tr>
    <td>DUPCOUNT</td>
    <td>get number of values stored at the given key (requires dupsort)</td>
    <td>key</td>
    <td>int: number of values, or None if key does not exist</td>
  </tr>
  <tr>
    <td>EXISTS</td>
    <td>determine if the given key exists</td>
    <td>key</td>
    <td>bool</td>
  </tr>
  <tr>
    <td>GET</td>
    <td>get the value associated with a given key. If dupsort is enabled and
    multiple values are present, the one that is sorted first will be returned.</td>
    <td>key</td>
    <td>value or None</td>
  </tr>
  <tr>
    <td>GETDUP</td>
    <td>get all values associated with a given key (requires dupsort)</td>
    <td>key</td>
    <td>list of values or None if key does not exist</td>
  </tr>
  <tr>
    <td>LENGTH</td>
    <td>get the length of the value stored at a given key, e.g. for a string
    this returns the number of characters, for a list the number of items, etc.</td>
    <td>key</td>
    <td>length of value or None if key does not exist</td>
  </tr>
  <tr>
    <td>POP</td>
    <td>atomically get and delete the value at a given key. If dupsort is
    enabled and multiple values are present, the one that is sorted first will
    be removed and returned.</td>
    <td>key</td>
    <td>value or None</td>
  </tr>
  <tr>
    <td>REPLACE</td>
    <td>atomically get and set the value at a given key. If dupsort is enabled,
    the first value will be returned (if exists) and ALL values will be removed
    so that only the new value is stored.</td>
    <td>key</td>
    <td>previous value or None</td>
  </tr>
  <tr>
    <td>SET</td>
    <td>store a key/value pair. If dupsort is enabled, duplicate values will be
    stored at the given key in sorted-order. Additionally, if dupsort is
    enabled and the exact key/value pair already exist, no changes are made.</td>
    <td>key, value</td>
    <td>int: 1 if new key/value added, 0 if dupsort is enabled and the
    key/value already exist</td>
  </tr>
  <tr>
    <td>SETDUP</td>
    <td>store a key/value pair, treating duplicates as successful writes
    (requires dupsort). Unlike SET, if the exact key/value pair already exists,
    this command will return 1 indicating success.</td>
    <td>key, value</td>
    <td>int: 1 on success</td>
  </tr>
  <tr>
    <td>SETDUPRAW</td>
    <td>store a key/value pair, treating duplicates as successful writes
    (requires dupsort). Additionally, the value is not serialized, but is
    stored as a raw bytestring.</td>
    <td>key, value (bytes)</td>
    <td>int: 1 on success</td>
  </tr>
  <tr>
    <td>SETNX</td>
    <td>store a key/value pair only if the key does not exist</td>
    <td>key, value</td>
    <td>int: 1 on success, 0 if key already exists</td>
  </tr>
  <tr>
    <th>Bulk KV commands</th>
  </tr>
  <tr>
    <td>MDELETE</td>
    <td>delete multiple keys</td>
    <td>list of keys</td>
    <td>int: number of keys deleted</td>
  </tr>
  <tr>
    <td>MGET</td>
    <td>get the value of multiple keys</td>
    <td>list of keys</td>
    <td>dict of key and value. Keys that were requested, but which do not
    exist are not included in the response.</td>
  </tr>
  <tr>
    <td>MGETDUP</td>
    <td>get all values of multiple keys (requires dupsort)</td>
    <td>list of keys</td>
    <td>dict of key to list of values. Keys that were requested, but which do
    not exist, are not included in the response.</td>
  </tr>
  <tr>
    <td>MPOP</td>
    <td>atomically get and delete the value of multiple keys. If dupsort is
    enabled and multiple values are stored at a given key, only the first value
    will be removed.</td>
    <td>list of keys</td>
    <td>dict of key to value</td>
  </tr>
  <tr>
    <td>MREPLACE</td>
    <td>atomically get and set the value of multiple keys. If dupsort is
    enabled and multiple values are stored at a given key, only the first value
    will be returned and all remaining values discarded.</td>
    <td>dict of key to value</td>
    <td>dict of key to previous value. Keys that did not exist previously will
    not be included in the response.</td>
  </tr>
  <tr>
    <td>MSET</td>
    <td>set the value of multiple keys.</td>
    <td>dict of key to value</td>
    <td>int: number of key / value pairs set</td>
  </tr>
  <tr>
    <td>MSETDUP</td>
    <td>store multiple key/value pairs, treating duplicates as successful
    writes (requires dupsort). Unlike MSET, if the exact key/value pair already
    exists, this command will treat the write as a success.</td>
    <td>dict of key to value</td>
    <td>int: number of key / value pairs set</td>
  </tr>
  <tr>
    <td>MSETNX</td>
    <td>store multiple key/value pair only if the key does not exist</td>
    <td>dict of key to value</td>
    <td>int: number of key / value pairs set</td>
  </tr>
  <tr>
    <th>Cursor / range commands</th>
  </tr>
  <tr>
    <td>DELETERANGE</td>
    <td>delete a range of keys using optional inclusive start/end-points</td>
    <td>start key (optional), end key (optional), count (optional)</td>
    <td>int: number of keys deleted</td>
  </tr>
  <tr>
    <td>GETRANGE</td>
    <td>retrieve a range of key/value pairs using optional inclusive
    start/end-points</td>
    <td>start key (optional), end key (optional), count (optional)</td>
    <td>list of [key, value] lists</td>
  </tr>
  <tr>
    <td>GETRANGEDUPRAW</td>
    <td>retrieve a range of duplicate values stored in a given key, using
    optional (inclusive) start/end-points</td>
    <td>key, start value (optional), end value (optional), count (optional)</td>
    <td>list of values (as bytestrings)</td>
  </tr>
  <tr>
    <td>KEYS</td>
    <td>retrieve a range of keys using optional inclusive start/end-points</td>
    <td>start key (optional), end key (optional), count (optional)</td>
    <td>list of keys</td>
  </tr>
  <tr>
    <td>PREFIX</td>
    <td>retrieve a range of key/value pairs which match the given prefix</td>
    <td>prefix, count (optional)</td>
    <td>list of [key, value] lists</td>
  </tr>
  <tr>
    <td>VALUES</td>
    <td>retrieve a range of values using optional inclusive start/end-points</td>
    <td>start key (optional), end key (optional), count (optional)</td>
    <td>list of values</td>
  </tr>
  <tr>
    <th>Client commands</th>
  </tr>
  <tr>
    <td>QUIT</td>
    <td>disconnect from server</td>
    <td>(none)</td>
    <td>int: 1</td>
  </tr>
  <tr>
    <td>SHUTDOWN</td>
    <td>terminate server process from client (be careful!)</td>
    <td>(none)</td>
    <td>(none)</td>
  </tr>
</table>

#### protocol

The protocol is the Redis RESPv3 protocol. I have extended the protocol with
one additional data-type, a dedicated type for representing UTF8-encoded
unicode text (notably absent from RESPv3). This type is denoted by the leading
`^` byte in responses.

Details can be found here: https://github.com/antirez/RESP3/blob/master/spec.md

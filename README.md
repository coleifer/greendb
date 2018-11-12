![](http://media.charlesleifer.com/blog/photos/logo-0.png)

### greendb

server frontend for symas lmdb.

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
  -m MAX_CLIENTS, --max-clients=MAX_CLIENTS
                        Maximum number of clients.
  -p PORT, --port=PORT  Port to listen on.
  -r, --reset           Reset database and config. All data will be lost.
```

Config file example with defaults -- remove comments before using:

```javascript

{
  "host": "127.0.0.1",
  "port": 31337,
  "max_clients": 1024,
  "path": "data",  // Directory for data storage, default is "data" in CWD.
  "map_size": 268435456,  // Default map size is 256MB. INCREASE THIS!
  "read_only": false,  // Open the database in read-only mode.
  "metasync": true,  // Sync metadata changes (recommended).
  "sync": true,  // Sync all changes.
  "writemap": false,  // Use a writable map (probably safe to do).
  "map_async": false,  // Asynchronous writable map.
  "meminit": true,  // Initialize new memory pages to zero.
  "max_dbs": 16,  // Maximum number of DBs.
  "max_spare_txns": 64,
  "lock": true,  // Lock the database when opening environment.
  "dupsort": false  // Either a boolean or a list of DB indexes.
}
```

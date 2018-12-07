"""
Test simpledb with many concurrent connections.
"""
from gevent import monkey; monkey.patch_all()

import time

import gevent
from greendb import Client

client = Client()

def get_sleep_set(k, v, n=1):
    client.set(k, v)
    #time.sleep(n)
    client._sleep(n)
    assert client.get(k) == v
    data = {b'%s-%032d' % (k, i): 'v%01024d' % i for i in range(100)}
    client.mset(data)
    resp = client.mget([b'%s-%032d' % (k, i) for i in range(100)])
    assert resp == data
    # client.close()  # If this were a real app, we would put this here.

n = 1
t = 100
start = time.time()

greenlets = []
for i in range(t):
    greenlets.append(gevent.spawn(get_sleep_set, b'k%d' % i, b'v%d' % i, n))

for g in greenlets:
    g.join()

client.flush()
stop = time.time()
print('done. slept=%s, took %.2f for %s threads.' % (n, stop - start, t))

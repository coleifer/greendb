"""
Test simpledb with many concurrent connections.
"""
from gevent import monkey; monkey.patch_all()

import time

import gevent
from greendb import Client




def get_sleep_set(k, v, n=1):
    client = Client()
    client.set(k, v)
    #time.sleep(n)
    client._sleep(n)
    assert client.get(k) == v
    client.close()


n = 1
t = 256
start = time.time()

greenlets = []
for i in range(t):
    greenlets.append(gevent.spawn(get_sleep_set, 'k%d' % i, 'v%d' % i, n))

for g in greenlets:
    g.join()

Client().flush()
stop = time.time()
print('done. slept=%s, took %.2f for %s threads.' % (n, stop - start, t))

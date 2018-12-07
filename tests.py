#!/usr/bin/env python

from __future__ import unicode_literals  # Required for 2.x compatability.

import logging
import os
import shutil
import sys
import tempfile
import unittest

import gevent

from greendb import Client
from greendb import CommandError
from greendb import Server
from greendb import logger
from greenquery import *


TEST_HOST = '127.0.0.1'
TEST_PORT = 31327


def run_server():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.ERROR)
    tmp_dir = tempfile.mkdtemp(suffix='greendb')
    data_dir = os.path.join(tmp_dir, 'data')
    server = Server(host=TEST_HOST, port=TEST_PORT, path=data_dir,
                    max_dbs=4, dupsort=[3])
    def run():
        try:
            server.run()
        finally:
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
    t = gevent.spawn(run)
    return t, server, tmp_dir


class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Client(host=TEST_HOST, port=TEST_PORT)

    @classmethod
    def tearDownClass(cls):
        cls.c.quit()

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
        self.c.flushall()


class TestBasicOperations(BaseTestCase):
    def setUp(self):
        super(TestBasicOperations, self).setUp()
        # By default we will use the 0th database.
        self.c.use(0)

    def test_ping(self):
        self.assertEqual(self.c.ping(), b'pong')

    def test_identity(self):
        test_data = (
            b'foo',
            b'\xff\x00\xff',
            'foo',
            '\u2012\u2013',
            0,
            1337,
            -1,
            3.14159,
            [b'foo', b'\xff\x00\xff', 31337, [b'bar']],
            {b'k1': b'v1', b'k2': 2, b'k3': {b'x3': b'y3'}},
            True,
            False,
            None,
            b'',
            b'a' * (1024 * 1024),  # 1MB value.
        )

        for test in test_data:
            self.assertEqual(self.c.set('key', test), 1)
            self.assertEqual(self.c.get('key'), test)

    def test_dict_interface(self):
        self.c['k1'] = 'v1'
        self.assertEqual(self.c['k1'], 'v1')
        self.assertFalse('k2' in self.c)
        self.c['k2'] = 'v2'
        self.assertTrue('k2' in self.c)
        self.assertEqual(list(self.c), [b'k1', b'k2'])
        self.assertEqual(len(self.c), 2)
        del self.c['k2']
        self.assertFalse('k2' in self.c)
        self.assertEqual(len(self.c), 1)

        self.c.update(k2='v2', k3='v3')
        self.assertEqual(self.c[:'k2'], [[b'k1', 'v1'], [b'k2', 'v2']])
        self.assertEqual(self.c['k1':'k2x'], [[b'k1', 'v1'], [b'k2', 'v2']])
        self.assertEqual(self.c['k1x':], [[b'k2', 'v2'], [b'k3', 'v3']])
        self.assertEqual(self.c['k1x'::1], [[b'k2', 'v2']])

        del self.c['k1x'::1]
        self.assertEqual(list(self.c), [b'k1', b'k3'])
        del self.c['k1', 'k2', 'k3', 'k4']
        self.assertEqual(list(self.c), [])

    def test_env_info(self):
        info = self.c.envinfo()
        self.assertEqual(info['clients'], 1)
        self.assertEqual(info['host'], '127.0.0.1')
        self.assertEqual(info['port'], TEST_PORT)
        sc = info['storage']
        self.assertEqual(sc['dupsort'], [3])
        self.assertEqual(sc['lock'], 1)
        self.assertEqual(sc['max_dbs'], 4)
        self.assertEqual(sc['sync'], 0)

    def test_decode_keys(self):
        c = Client(host=TEST_HOST, port=TEST_PORT, decode_keys=True)
        c.mset({'k1': b'v1', 'k2': {'x1': 'y1', 'x2': {'y2': b'z2'}}})
        c.mset({'i1': {1: 2}, 'k3': ['foo', {b'a': 'b'}, b'bar']})

        # Dicts are decoded recursively.
        self.assertEqual(c.mget(['k1', 'k2']), {
            'k1': b'v1',
            'k2': {'x1': 'y1', 'x2': {'y2': b'z2'}}})

        # Nested dicts are not decoded.
        self.assertEqual(c.get('k3'), ['foo', {b'a': 'b'}, b'bar'])

        # ints and other values are modified, as keys must be strings.
        self.assertEqual(c.get('i1'), {'1': 2})
        c.close()

    def test_crud(self):
        # Setting a key/value returns number of keys set.
        self.assertEqual(self.c.set('k1', b'v1'), 1)

        # We can verify the key exists, and that we can retrieve our value.
        self.assertTrue(self.c.exists('k1'))
        self.assertEqual(self.c.get('k1'), b'v1')
        self.assertEqual(self.c.count(), 1)

        # Deleting returns the number of keys deleted.
        self.assertEqual(self.c.delete('k1'), 1)
        self.assertFalse(self.c.exists('k1'))
        self.assertEqual(self.c.count(), 0)

        # Subsequent call to delete returns 0.
        self.assertEqual(self.c.delete('k1'), 0)

        # Getting a nonexistant key returns None.
        self.assertTrue(self.c.get('k1') is None)

        # Let's set and then update a key.
        self.assertEqual(self.c.set('key', 'ccc'), 1)
        self.assertEqual(self.c.get('key'), 'ccc')

        # dupsort is disabled for this database, so the value is replaced.
        self.assertEqual(self.c.set('key', 'ddd'), 1)
        self.assertEqual(self.c.get('key'), 'ddd')
        self.assertEqual(self.c.set('key', 'bbb'), 1)
        self.assertEqual(self.c.get('key'), 'bbb')
        self.assertEqual(self.c.getdup('key'), ['bbb'])
        self.assertRaises(CommandError, self.c.dupcount, 'key')

        # We can set to the same value and the db returns 1. When dupsort is
        # enabled, this returns 0.
        self.assertEqual(self.c.set('key', 'bbb'), 1)
        self.assertEqual(self.c.getdup('key'), ['bbb'])

        self.assertEqual(self.c.pop('key'), 'bbb')
        self.assertTrue(self.c.pop('key') is None)

        self.assertTrue(self.c.replace('key', 'aaa') is None)
        self.assertEqual(self.c.get('key'), 'aaa')
        self.assertEqual(self.c.replace('key', 'bbb'), 'aaa')
        self.assertEqual(self.c.get('key'), 'bbb')
        self.assertTrue(self.c.replace('keyz', 'xxx') is None)
        self.assertEqual(self.c.replace('keyz', 'yyy'), 'xxx')

        self.assertEqual(self.c.setnx('key', 'ccc'), 0)
        self.assertEqual(self.c.setnx('key2', 'xxx'), 1)
        self.assertEqual(self.c.get('key'), 'bbb')
        self.assertEqual(self.c.get('key2'), 'xxx')

    def test_crud_dupsort(self):
        # Use the DB with dupsort enabled.
        self.c.use(3)

        # Setting a key/value returns number of keys set.
        self.assertEqual(self.c.set('k1', b'v1'), 1)

        # We can verify the key exists, and that we can retrieve our value.
        self.assertTrue(self.c.exists('k1'))
        self.assertEqual(self.c.get('k1'), b'v1')
        self.assertEqual(self.c.count(), 1)

        # We can add another value with dupsort enabled.
        self.assertEqual(self.c.set('k1', b'v1-x'), 1)

        # Deleting returns the number of keys deleted.
        self.assertEqual(self.c.delete('k1'), 1)
        self.assertFalse(self.c.exists('k1'))
        self.assertEqual(self.c.count(), 0)

        # Subsequent call to delete returns 0.
        self.assertEqual(self.c.delete('k1'), 0)

        # Set multiple values and then use deletedup to verify the old values
        # are preserved.
        self.c.set('k1', b'v1-a')
        self.c.set('k1', b'v1-b')
        self.c.set('k1', b'v1-c')
        self.assertEqual(self.c.deletedup('k1', b'v1-b'), 1)
        self.assertEqual(self.c.deletedup('k1', b'v1-x'), 0)
        self.assertTrue(self.c.exists('k1'))
        self.assertEqual(self.c.count(), 2)
        self.assertEqual(self.c.getdup('k1'), [b'v1-a', b'v1-c'])
        self.assertEqual(self.c.delete('k1'), 1)
        self.assertEqual(self.c.count(), 0)

        # Getting a nonexistant key returns None.
        self.assertTrue(self.c.get('k1') is None)

        # Let's set and then update a key.
        self.assertEqual(self.c.set('key', b'ccc'), 1)
        self.assertEqual(self.c.get('key'), b'ccc')

        # Because our databases use dupsort and multi-value, we actually get
        # "ccc" here, because "ccc" sorts before "ddd".
        self.assertEqual(self.c.set('key', b'ddd'), 1)
        self.assertEqual(self.c.get('key'), b'ccc')
        self.assertEqual(self.c.set('key', b'bbb'), 1)
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.getdup('key'), [b'bbb', b'ccc', b'ddd'])
        self.assertEqual(self.c.dupcount('key'), 3)

        # However we can't set the same exact key/data, except via setdup:
        self.assertEqual(self.c.set('key', b'ccc'), 0)
        self.assertEqual(self.c.set('key', b'bbb'), 0)
        self.assertEqual(self.c.getdup('key'), [b'bbb', b'ccc', b'ddd'])
        self.assertEqual(self.c.dupcount('key'), 3)

        self.assertEqual(self.c.pop('key'), b'bbb')
        self.assertEqual(self.c.pop('key'), b'ccc')
        self.assertEqual(self.c.pop('key'), b'ddd')
        self.assertTrue(self.c.pop('key') is None)

        self.assertTrue(self.c.replace('key', b'aaa') is None)
        self.assertEqual(self.c.get('key'), b'aaa')
        self.assertEqual(self.c.replace('key', b'bbb'), b'aaa')
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.getdup('key'), [b'bbb'])
        self.assertEqual(self.c.dupcount('key'), 1)
        self.assertTrue(self.c.replace('keyz', b'xxx') is None)
        self.assertEqual(self.c.replace('keyz', b'yyy'), b'xxx')
        self.assertEqual(self.c.getdup('keyz'), [b'yyy'])

        self.assertEqual(self.c.setnx('key', b'ccc'), 0)
        self.assertEqual(self.c.setnx('key2', b'xxx'), 1)
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.getdup('key'), [b'bbb'])
        self.assertEqual(self.c.get('key2'), b'xxx')

    def test_bulk_operations(self):
        self.assertEqual(self.c.mset({'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}),
                         3)
        self.assertEqual(self.c.mset({
            'k1': b'v1-x',
            'k2': b'v2',
            'k4': b'v4',
            'k5': b'v5'}), 4)

        self.assertEqual(self.c.mget(['k1', 'k3', 'k5']), {
            b'k1': b'v1-x',
            b'k3': b'v3',
            b'k5': b'v5'})
        self.assertEqual(self.c.mget(['k0', 'k2', 'kx']), {b'k2': b'v2'})
        self.assertEqual(self.c.mget(['kx', 'ky', 'kz']), {})

        # Bulk delete returns number actually deleted.
        self.assertEqual(self.c.mdelete(['k2', 'k5', 'kx']), 2)
        self.assertEqual(self.c.mdelete(['k2', 'k5', 'kx']), 0)

        # Bulk pop.
        self.assertEqual(self.c.mpop(['k3', 'k4', 'kz']),
                         {b'k3': b'v3', b'k4': b'v4'})
        self.assertEqual(self.c.mpop(['k3', 'k4', 'kz']), {})

        # Bulk replace, returns the previous value (if it exists).
        res = self.c.mreplace({'k3': b'v3-x', 'k4': b'v4-x', 'k1': b'v1-z'})
        self.assertEqual(res, {b'k1': b'v1-x'})

        res = self.c.mreplace({'k3': b'v3-y', 'k4': b'v4-y'})
        self.assertEqual(res, {b'k3': b'v3-x', b'k4': b'v4-x'})

        # Set NX.
        self.assertEqual(self.c.msetnx({'k2': b'v2', 'k4': b'v4'}), 1)
        self.assertEqual(self.c.msetnx({'k2': b'v2', 'k3': b'v3'}), 0)
        self.assertEqual(self.c.mget(['k1', 'k2', 'k3', 'k4', 'k5']), {
            b'k1': b'v1-z',
            b'k2': b'v2',
            b'k3': b'v3-y',
            b'k4': b'v4-y'})

    def test_bulk_operations_dupsort(self):
        # We need to specify the DB that supports dupsort.
        self.c.use(3)

        self.assertEqual(self.c.mset({'k1': b'v1', 'k2': b'v2', 'k3': b'v3'}),
                         3)
        self.assertEqual(self.c.mset({
            'k1': b'v1-x',
            'k2': b'v2',
            'k4': b'v4',
            'k5': b'v5'}), 3)  # Only k1, k4 and k5 are counted: k2 is same.
        self.assertEqual(self.c.mset({'k5': b'v5-x'}), 1)

        self.assertEqual(self.c.mget(['k1', 'k3', 'k5']), {
            b'k1': b'v1',  # v1-x sorts *after* v1.
            b'k3': b'v3',
            b'k5': b'v5'})
        self.assertEqual(self.c.mget(['k0', 'k2', 'kx']), {b'k2': b'v2'})
        self.assertEqual(self.c.mget(['kx', 'ky', 'kz']), {})

        # We can get all dupes using mgetdup.
        self.assertEqual(self.c.mgetdup(['k1', 'k3', 'k5', 'kx']), {
            b'k1': [b'v1', b'v1-x'],
            b'k3': [b'v3'],
            b'k5': [b'v5', b'v5-x']})

        # Bulk delete returns number actually deleted. Event though k5 has two
        # values, it is only counted once here.
        self.assertEqual(self.c.mdelete(['k2', 'k5', 'kx']), 2)
        self.assertEqual(self.c.mdelete(['k2', 'k5', 'kx']), 0)

        # Bulk pop.
        self.assertEqual(self.c.mpop(['k3', 'k4', 'kz']),
                         {b'k3': b'v3', b'k4': b'v4'})
        self.assertEqual(self.c.mpop(['k3', 'k4', 'kz']), {})

        # Bulk pop with duplicates. Only the first value is popped off.
        self.c.mset({'k5': b'v5-a'})
        self.c.mset({'k5': b'v5-b'})
        self.assertEqual(self.c.mpop(['k1', 'k5']),
                         {b'k1': b'v1', b'k5': b'v5-a'})
        self.assertEqual(self.c.mpop(['k1', 'k5']),
                         {b'k1': b'v1-x', b'k5': b'v5-b'})
        self.assertEqual(self.c.mpop(['k1', 'k5']), {})

        # Restore the values to k1.
        self.c.set('k1', b'v1-x')

        # Bulk replace, returns the previous value (if it exists).
        res = self.c.mreplace({'k3': b'v3-x', 'k4': b'v4-x', 'k1': b'v1-z'})
        self.assertEqual(res, {b'k1': b'v1-x'})

        # Although we overwrote k1, the original value is not preserved!
        self.assertEqual(self.c.getdup('k1'), [b'v1-z'])
        self.assertEqual(self.c.dupcount('k1'), 1)

        # Verify the original values are returned.
        res = self.c.mreplace({'k3': b'v3-y', 'k4': b'v4-y'})
        self.assertEqual(res, {b'k3': b'v3-x', b'k4': b'v4-x'})

        # Set NX works the same as non-dupsort.
        self.assertEqual(self.c.msetnx({'k2': b'v2', 'k4': b'v4'}), 1)
        self.assertEqual(self.c.msetnx({'k2': b'v2', 'k3': b'v3'}), 0)
        self.assertEqual(self.c.mget(['k1', 'k2', 'k3', 'k4', 'k5']), {
            b'k1': b'v1-z',
            b'k2': b'v2',
            b'k3': b'v3-y',
            b'k4': b'v4-y'})

    def test_match_prefix(self):
        indices = list(range(0, 100, 5))

        # k000, k005, k010, k015 ... k090, k095.
        self.c.mset({'k%03d' % i: b'v%d' % i for i in indices})

        def assertPrefix(prefix, indices, count=None):
            r = self.c.prefix(prefix, count)
            if count is not None:
                indices = indices[:count]
            self.assertEqual(r, [[b'k%03d' % i, b'v%d' % i] for i in indices])

        for count in (None, 1, 2, 100):
            # Prefix scan works as expected.
            assertPrefix('k01', [10, 15])
            assertPrefix('k00', [0, 5])
            assertPrefix('k020', [20])
            assertPrefix('k025', [25])

            # No keys match these prefixes.
            assertPrefix('k021', [])
            assertPrefix('k001', [])
            assertPrefix('k1', [])
            assertPrefix('j', [])

            # These prefixes match all keys.
            assertPrefix('', indices)
            assertPrefix('k', indices)
            assertPrefix('k0', indices)

    def test_match_prefix_dupsort(self):
        self.c.use(3)
        self.c.mset({'aaa': b'a1', 'aab': b'b1', 'aac': b'c1'})
        self.c.mset({'aaa': b'a2', 'aac': b'c2'})
        items = [
            [b'aaa', b'a1'], [b'aaa', b'a2'],
            [b'aab', b'b1'],
            [b'aac', b'c1'], [b'aac', b'c2']]

        self.assertEqual(self.c.prefix('aa'), items)
        self.assertEqual(self.c.prefix('aa', 3), items[:3])
        self.assertEqual(self.c.prefix('aaa', 3), items[:2])
        self.assertEqual(self.c.prefix('aaa', 1), items[:1])

    def test_deleterange(self):
        self.c.mset(dict(('k%d' % i, b'v%d' % i) for i in range(20)))
        self.assertEqual(self.c.deleterange('k18', 'k3'), 4)  # 18, 19, 2, 3.
        self.assertEqual(self.c.deleterange('k17x', 'k3x'), 0)

        self.assertEqual(self.c.deleterange('k10', 'k15', 2), 2)  # 10, 11.
        self.assertEqual(self.c.deleterange('k10', 'k15', 2), 2)  # 12, 13.
        r = self.c.getrange('k0', 'k15')
        self.assertEqual([k for k, _ in r], [b'k0', b'k1', b'k14', b'k15'])
        self.assertEqual(self.c.deleterange('k0', 'k15'), 4)

        self.assertEqual(self.c.deleterange(None, 'k0'), 0)
        self.assertEqual(self.c.deleterange('k9z', None), 0)
        self.assertEqual(self.c.deleterange('a0', 'a9'), 0)
        self.assertEqual(self.c.deleterange('z0', 'z9'), 0)
        self.assertEqual(self.c.deleterange(), 8)

    def test_deleterange_dupsort(self):
        self.assertEqual(self.c.use(3), 3)  # Use dupsort db.
        self.c.mset({'k0': b'v0', 'k1': b'v1', 'k10': b'v10', 'k11': b'v11'})
        self.c.mset({'k0': b'v0-x', 'k10': b'v10-x', 'k11': b'v11-x'})
        self.c.mset({'k0': b'v0-y', 'k11': b'v11-y'})

        # k0 [v0,v0-x,v0-y], k1 [v1], k10 [v10,v10-x], k11 [v11,v11-x,v11-y]
        self.assertEqual(self.c.count(), 9)
        self.assertEqual(self.c.deleterange('k1', 'k9', 2), 2)
        self.assertEqual([v for _, v in self.c.getrange('k1', 'k9')],
                         [b'v10-x', b'v11', b'v11-x', b'v11-y'])

        self.assertEqual(self.c.deleterange('k1', 'k9', 3), 3)
        self.assertEqual([v for _, v in self.c.getrange()],
                         [b'v0', b'v0-x', b'v0-y', b'v11-y'])

        self.assertEqual(self.c.deleterange(None, None, 2), 2)
        self.assertEqual([v for _, v in self.c.getrange()],
                         [b'v0-y', b'v11-y'])

        self.assertEqual(self.c.deleterange(None, None, 4), 2)
        self.assertEqual(self.c.count(), 0)

    def test_getrange_keys_values(self):
        self.c.mset(dict(('k%d' % i, b'v%d' % i) for i in range(20)))
        sorted_values = list(map(int, sorted(map(str, range(20)))))

        def assertRange(start, end, indices, count=None):
            if count is not None:
                indices = indices[:count]

            # Test KEYS command.
            res = self.c.keys(start, end, count)
            self.assertEqual(res, [b'k%d' % i for i in indices])

            # Test VALUES command.
            res = self.c.values(start, end, count)
            self.assertEqual(res, [b'v%d' % i for i in indices])

            # Test GETRANGE command.
            res = self.c.getrange(start, end, count)
            self.assertEqual(res, [[b'k%d' % i, b'v%d' % i] for i in indices])

        for count in (None, 1, 2, 100):
            assertRange('k3', 'k6', [3, 4, 5, 6], count)
            assertRange('k18', 'k3', [18, 19, 2, 3], count)
            assertRange('k3x', 'k6x', [4, 5, 6], count)
            assertRange('k0', 'k12', [0, 1, 10, 11, 12], count)
            assertRange('k01', 'k121', [1, 10, 11, 12], count)

            # Test boundaries.
            assertRange(None, None, sorted_values, count)
            assertRange(None, 'kz', sorted_values, count)
            assertRange('a0', None, sorted_values, count)
            assertRange('k0', None, sorted_values, count)
            assertRange('k0', 'k9', sorted_values, count)

            # Test out-of-bounds.
            assertRange(None, 'a0', [], count)
            assertRange('z0', None, [], count)
            assertRange('a0', 'a99', [], count)
            assertRange('z0', 'z99', [], count)

    def test_getrange_keys_values_dupsort(self):
        self.c.use(3)
        nums = [0, 1, 10, 2, 3, 4]
        self.c.mset(dict(('k%d' % i, b'v%d' % i) for i in nums))
        self.c.mset(dict(('k%d' % i, b'v%d-x' % i) for i in nums if not i % 2))

        def assertRange(start, end, indices, count=None):
            accum = []
            for i in indices:
                accum.append([b'k%d' % i, b'v%d' % i])
                if i % 2 == 0:
                    accum.append([b'k%d' % i, b'v%d-x' % i])
            if count is not None:
                accum = accum[:count]

            res = self.c.getrange(start, end, count)
            self.assertEqual(res, accum)

            res = self.c.keys(start, end, count)
            self.assertEqual(res, [k for k, _ in accum])

            res = self.c.values(start, end, count)
            self.assertEqual(res, [v for _, v in accum])

        for count in (None, 1, 2, 100):
            assertRange('k2', 'k4', [2, 3, 4], count)
            assertRange('k1', 'k3', [1, 10, 2, 3], count)
            assertRange('k2x', 'k4x', [3, 4], count)
            assertRange('k0', 'k12', [0, 1, 10], count)
            assertRange('k01', 'k101', [1, 10], count)

            # Test boundaries.
            assertRange(None, None, nums, count)
            assertRange(None, 'kz', nums, count)
            assertRange('a0', None, nums, count)
            assertRange('k0', None, nums, count)
            assertRange('k0', 'k9', nums, count)

            # Test out-of-bounds.
            assertRange(None, 'a0', [], count)
            assertRange('z0', None, [], count)
            assertRange('a0', 'a99', [], count)
            assertRange('z0', 'z99', [], count)

    def test_incr_decr(self):
        self.assertEqual(self.c.incr('i0'), 1)
        self.assertEqual(self.c.incr('i0', 2), 3)
        self.assertEqual(self.c.incr('i1', 2), 2)
        self.assertEqual(self.c.incr('i1', 2.5), 4.5)
        self.assertEqual(self.c.decr('i1', 3.5), 1.0)
        self.assertEqual(self.c.decr('i0'), 2)
        self.assertEqual(self.c.decr('i2'), -1)
        self.assertEqual(self.c.get('i0'), 2)
        self.assertEqual(self.c.get('i1'), 1.)
        self.assertEqual(self.c.get('i2'), -1)
        self.c.set('i2', -2)
        self.assertEqual(self.c.decr('i2'), -3)
        self.c.set('i1', 2.0)
        self.assertEqual(self.c.incr('i1'), 3.)

    def test_cas(self):
        self.c.set('k1', b'v1')
        self.c.set('k2', b'v2')
        self.assertTrue(self.c.cas('k1', b'v1', b'v1-x'))
        self.assertFalse(self.c.cas('k1', b'v1', b'v1-y'))
        self.assertEqual(self.c.get('k1'), b'v1-x')

        self.assertFalse(self.c.cas('k2', b'v1-x', b'v1-z'))
        self.assertEqual(self.c.get('k2'), b'v2')
        self.assertTrue(self.c.cas('k2', b'v2', b'v2-x'))
        self.assertTrue(self.c.cas('k2', b'v2-x', b'v2-y'))
        self.assertFalse(self.c.cas('k2', b'v2', b'v2-z'))
        self.assertEqual(self.c.get('k2'), b'v2-y')

        self.assertFalse(self.c.cas('k3', b'', b'v3'))
        self.assertFalse(self.c.cas('k3', b'x3', b'y3'))
        self.assertTrue(self.c.cas('k3', None, b'v3'))
        self.assertEqual(self.c.get('k3'), b'v3')

    def test_cas_dupsort(self):
        self.c.use(3)

        self.c.set('k1', b'v1-b')
        self.c.set('k1', b'v1-a')
        self.c.set('k1', b'v1-c')
        self.c.set('k2', b'v2-a')

        # v1-a is the first element, so that is what we compare.
        self.assertFalse(self.c.cas('k1', b'v1-b', b'v1-x'))

        # We will successfully match value, so v1-a is deleted and v1-y is
        # added. The next compare will have to be against v1-b, however.
        self.assertTrue(self.c.cas('k1', b'v1-a', b'v1-y'))
        self.assertEqual(self.c.getdup('k1'), [b'v1-b', b'v1-c', b'v1-y'])
        self.assertFalse(self.c.cas('k1', b'v1-y', b'v1-z'))
        self.assertTrue(self.c.cas('k1', b'v1-b', b'v1-0'))
        self.assertEqual(self.c.getdup('k1'), [b'v1-0', b'v1-c', b'v1-y'])

        self.assertFalse(self.c.cas('k2', b'v1-0', b'v2-z'))
        self.assertEqual(self.c.getdup('k2'), [b'v2-a'])
        self.assertTrue(self.c.cas('k2', b'v2-a', b'v2-x'))
        self.assertTrue(self.c.cas('k2', b'v2-x', b'v2-y'))
        self.assertEqual(self.c.getdup('k2'), [b'v2-y'])

        self.assertFalse(self.c.cas('k3', b'', b'v3'))
        self.assertFalse(self.c.cas('k3', b'x3', b'y3'))
        self.assertTrue(self.c.cas('k3', None, b'v3'))
        self.assertEqual(self.c.getdup('k3'), [b'v3'])

    def test_length(self):
        self.c.set('k1', b'abc')
        self.c.set('k2', 'defghijkl')
        self.c.set('k3', b'')
        self.c.set('k4', [0, 1, 2, 3])
        self.c.set('k5', {'x': 'y', 'z': 'w'})
        self.assertEqual(self.c.length('k1'), 3)
        self.assertEqual(self.c.length('k2'), 9)
        self.assertEqual(self.c.length('k3'), 0)
        self.assertEqual(self.c.length('k4'), 4)
        self.assertEqual(self.c.length('k5'), 2)
        self.assertTrue(self.c.length('kx') is None)

    def test_raw_operations_dupsort(self):
        # We need to specify the DB that supports dupsort.
        self.c.use(3)

        data = (
            # id, username, status
            (1, 'huey', 1),
            (2, 'zaizee', 1),
            (3, 'mickey', 0),
            (4, 'beanie', 1),
            (5, 'gracie', 0),
            (6, 'rocky', 0),
            (7, 'rocky-2', 0),
            (8, 'rocky-3', 0))
        for pk, username, status in data:
            self.c.setdupraw('u:username', '%s %s' % (username, pk))
            self.c.setdupraw('u:status', '%s %s' % (status, pk))

        # We'll use chr(32) " " for the lower-bound, and chr(126) "~" for the
        # upper-bound.
        def assertR(start, stop, expected):
            res = self.c.getrangedupraw('u:username', start, stop)
            pks = [int(r.decode('utf8').rsplit(' ', 1)[1]) for r in res]
            self.assertEqual(pks, expected)

        # Verify inclusiveness of both boundaries in this scheme.
        assertR('zaizee ', 'zaizee ', [])
        assertR('zaizee ', 'zaizee 0', [])
        assertR('zaizee ', 'zaizee 2', [2])
        assertR('zaizee ', 'zaizee ~', [2])
        assertR('zaizee 0', 'zaizee ~', [2])
        assertR('zaizee 2', 'zaizee ~', [2])

        # Test equality comparison.
        assertR('zaizee ', 'zaizee ~', [2])

        # Less-than, and less-than-or-equal.
        assertR(None, 'rocky  ', [4, 5, 1, 3])
        assertR(None, 'rocky ~', [4, 5, 1, 3, 6])

        # Greater-than, and greater-than-or-equal.
        assertR('rocky ~', None, [7, 8, 2])
        assertR('rocky  ', None, [6, 7, 8, 2])

        # Startswith (prefix search).
        assertR('rocky', 'rocky~', [6, 7, 8])
        assertR('b', 'b~', [4])

        # Between inclusive, between exclusive.
        assertR('huey  ', 'rocky ~', [1, 3, 6])
        assertR('huey ~', 'rocky  ', [3])

        # Delete a few items and verify deletedupraw works.
        self.c.deletedupraw('u:username', 'huey 1')
        self.c.deletedupraw('u:username', 'rocky-2 7')
        self.c.deletedupraw('u:username', 'rocky-3 x')  # Does not match!
        assertR('gracie', 'zaizee  ', [5, 3, 6, 8])

        # Status test.
        def assertS(start, stop, expected):
            res = self.c.getrangedupraw('u:status', start, stop)
            pks = [int(r.decode('utf8').rsplit(' ', 1)[1]) for r in res]
            self.assertEqual(pks, expected)

        assertS('0 ', '0 ~', [3, 5, 6, 7, 8])
        assertS('1 ', '1 ~', [1, 2, 4])
        assertS('0 ', '1 ~', [3, 5, 6, 7, 8, 1, 2, 4])

    def test_processing_instruction_use_db(self):
        # Verify we can specify the database for a one-off operation.
        for i in range(4):
            self.c.set('k1', 'v1-%s' % i, db=i)
        for i in range(4):
            self.assertEqual(self.c.get('k1', db=i), 'v1-%s' % i)

        # The default db is 0.
        self.assertEqual(self.c.get('k1'), 'v1-0')

        # We can explicitly switch the db.
        self.assertEqual(self.c.use(2), 2)
        self.assertEqual(self.c.get('k1'), 'v1-2')

        # We can specify the db for a one-off command.
        for i in range(4):
            self.assertEqual(self.c.get('k1', db=i), 'v1-%s' % i)

        # The value from our call to "use()" is preserved.
        self.assertEqual(self.c.get('k1'), 'v1-2')


class Base(Model):
    class Meta:
        client = Client(host=TEST_HOST, port=TEST_PORT)
        database = 0
        index_db = 3

class User(Base):
    username = Field(index=True)
    status = IntegerField(index=True)

class Misc(Base):
    f = Field()
    f_i = IntegerField(index=True)
    f_l = LongField(index=True)
    f_dt = DateTimeField(index=True)
    f_ts = TimestampField(index=True)
    f_b = BooleanField()


class TestGreenQuery(BaseTestCase):
    def test_basic_operations(self):
        username_status = (
            ('huey', 1),
            ('mickey', 0),
            ('zaizee', 1),
            ('beanie', 2),
            ('gracie', 0),
            ('rocky', 0),
            ('rocky-2', 0),
        )
        for username, status in username_status:
            User.create(username=username, status=status)

        u_db = User.load(2)
        self.assertEqual(u_db.id, 2)
        self.assertEqual(u_db.username, 'mickey')
        self.assertEqual(u_db.status, 0)

        self.assertRaises(KeyError, User.load, 99)

        # We can retrieve all users, sorted by primary key.
        all_users = [user.username for user in User.all()]
        self.assertEqual(all_users, [u for u, _ in username_status])

        # We can apply a limit.
        some_users = [user.username for user in User.all(3)]
        self.assertEqual(some_users, ['huey', 'mickey', 'zaizee'])

        # Query a single object using equality.
        huey = User.get(User.username == 'huey')
        self.assertEqual(huey.id, 1)
        self.assertEqual(huey.username, 'huey')

        def assertQ(expr, usernames):
            query = User.query(expr)
            self.assertEqual([u.username for u in query], usernames)

        # Query using ranges.
        assertQ(User.username.between('huey', 'rocky'), ['huey', 'mickey'])
        assertQ(User.username.between('huey', 'rocky', True, True),
                ['huey', 'mickey', 'rocky'])  # Inclusive.
        assertQ((User.username >= 'rocky'), ['rocky', 'rocky-2', 'zaizee'])
        assertQ((User.username > 'rocky'), ['rocky-2', 'zaizee'])
        assertQ((User.username <= 'rocky'),
                ['beanie', 'gracie', 'huey', 'mickey', 'rocky'])
        assertQ((User.username < 'rocky'),
                ['beanie', 'gracie', 'huey', 'mickey'])
        assertQ((User.username == 'mickey'), ['mickey'])
        assertQ(User.username.startswith('ro'), ['rocky', 'rocky-2'])
        assertQ(User.username.startswith('rocky'), ['rocky', 'rocky-2'])
        assertQ(User.username.startswith('rocky '), [])

        assertQ((User.status > 1), ['beanie'])
        assertQ((User.status >= 1), ['huey', 'zaizee', 'beanie'])
        assertQ((User.status < 1), ['mickey', 'gracie', 'rocky', 'rocky-2'])
        assertQ((User.status != 0), ['huey', 'zaizee', 'beanie'])

        # Delete objects.
        for username in ('rocky', 'rocky-2', 'gracie', 'beanie'):
            user = User.get(User.username == username)
            user.delete()

        # Validate query results after deletions.
        self.assertEqual([u.username for u in User.all()],
                         ['huey', 'mickey', 'zaizee'])
        assertQ(User.username.between('huey', 'rocky', True, True),
                ['huey', 'mickey'])
        assertQ((User.username >= 'rocky'), ['zaizee'])
        assertQ((User.username > 'rocky'), ['zaizee'])
        assertQ((User.username <= 'rocky'), ['huey', 'mickey'])
        assertQ((User.username < 'rocky'), ['huey', 'mickey'])
        assertQ((User.username == 'mickey'), ['mickey'])
        assertQ(User.username.startswith('h'), ['huey'])
        assertQ(User.username.startswith('r'), [])

        assertQ((User.status > 1), [])
        assertQ((User.status >= 1), ['huey', 'zaizee'])
        assertQ((User.status < 1), ['mickey'])
        assertQ((User.status != 0), ['huey', 'zaizee'])


if __name__ == '__main__':
    server_t, server, tmp_dir = run_server()
    unittest.main(argv=sys.argv)

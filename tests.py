#!/usr/bin/env python

import logging
import os
import shutil
import sys
import tempfile
import unittest

import gevent

from clowndb import Client
from clowndb import CommandError
from clowndb import Server
from clowndb import logger


TEST_HOST = '127.0.0.1'
TEST_PORT = 31327


def run_server():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.ERROR)
    tmp_dir = tempfile.mkdtemp(suffix='clowndb')
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

    def test_identity(self):
        test_data = (
            b'foo',
            b'\xff\x00\xff',
            0,
            1337,
            -1,
            3.14159,
            [b'foo', b'\xff\x00\xff', 31337, [b'bar']],
            {b'k1': b'v1', b'k2': 2, b'k3': {b'x3': b'y3'}},
            None,
            b'',
            b'a' * (1024 * 1024),  # 1MB value.
        )

        for test in test_data:
            self.assertEqual(self.c.set('key', test), 1)
            self.assertEqual(self.c.get('key'), test)

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
        self.assertEqual(self.c.get('key'), b'ccc')

        # dupsort is disabled for this database, so the value is replaced.
        self.assertEqual(self.c.set('key', 'ddd'), 1)
        self.assertEqual(self.c.get('key'), b'ddd')
        self.assertEqual(self.c.set('key', 'bbb'), 1)
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.getdup('key'), [b'bbb'])
        self.assertRaises(CommandError, self.c.dupcount, 'key')

        # We can set to the same value and the db returns 1. When dupsort is
        # enabled, this returns 0.
        self.assertEqual(self.c.set('key', 'bbb'), 1)
        self.assertEqual(self.c.getdup('key'), [b'bbb'])

        self.assertEqual(self.c.pop('key'), b'bbb')
        self.assertTrue(self.c.pop('key') is None)

        self.assertTrue(self.c.replace('key', 'aaa') is None)
        self.assertEqual(self.c.get('key'), b'aaa')
        self.assertEqual(self.c.replace('key', 'bbb'), b'aaa')
        self.assertEqual(self.c.get('key'), b'bbb')

        self.assertEqual(self.c.setnx('key', 'ccc'), 0)
        self.assertEqual(self.c.setnx('key2', 'xxx'), 1)
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.get('key2'), b'xxx')

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
        self.c.set('k1', 'v1-a')
        self.c.set('k1', 'v1-b')
        self.c.set('k1', 'v1-c')
        self.assertEqual(self.c.deletedup('k1', 'v1-b'), 1)
        self.assertEqual(self.c.deletedup('k1', 'v1-x'), 0)
        self.assertTrue(self.c.exists('k1'))
        self.assertEqual(self.c.count(), 2)
        self.assertEqual(self.c.getdup('k1'), [b'v1-a', b'v1-c'])
        self.assertEqual(self.c.delete('k1'), 1)
        self.assertEqual(self.c.count(), 0)

        # Getting a nonexistant key returns None.
        self.assertTrue(self.c.get('k1') is None)

        # Let's set and then update a key.
        self.assertEqual(self.c.set('key', 'ccc'), 1)
        self.assertEqual(self.c.get('key'), b'ccc')

        # Because our databases use dupsort and multi-value, we actually get
        # "ccc" here, because "ccc" sorts before "ddd".
        self.assertEqual(self.c.set('key', 'ddd'), 1)
        self.assertEqual(self.c.get('key'), b'ccc')
        self.assertEqual(self.c.set('key', 'bbb'), 1)
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.getdup('key'), [b'bbb', b'ccc', b'ddd'])
        self.assertEqual(self.c.dupcount('key'), 3)

        # However we can't set the same exact key/data, except via setdup:
        self.assertEqual(self.c.set('key', 'ccc'), 0)
        self.assertEqual(self.c.set('key', 'bbb'), 0)
        self.assertEqual(self.c.getdup('key'), [b'bbb', b'ccc', b'ddd'])
        self.assertEqual(self.c.dupcount('key'), 3)

        self.assertEqual(self.c.pop('key'), b'bbb')
        self.assertEqual(self.c.pop('key'), b'ccc')
        self.assertEqual(self.c.pop('key'), b'ddd')
        self.assertTrue(self.c.pop('key') is None)

        self.assertTrue(self.c.replace('key', 'aaa') is None)
        self.assertEqual(self.c.get('key'), b'aaa')
        self.assertEqual(self.c.replace('key', 'bbb'), b'aaa')
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.getdup('key'), [b'bbb'])
        self.assertEqual(self.c.dupcount('key'), 1)

        self.assertEqual(self.c.setnx('key', 'ccc'), 0)
        self.assertEqual(self.c.setnx('key2', 'xxx'), 1)
        self.assertEqual(self.c.get('key'), b'bbb')
        self.assertEqual(self.c.getdup('key'), [b'bbb'])
        self.assertEqual(self.c.get('key2'), b'xxx')

    def test_bulk_operations(self):
        self.assertEqual(self.c.mset({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}), 3)
        self.assertEqual(self.c.mset({
            'k1': 'v1-x',
            'k2': 'v2',
            'k4': 'v4',
            'k5': 'v5'}), 4)

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
        res = self.c.mreplace({'k3': 'v3-x', 'k4': 'v4-x', 'k1': 'v1-z'})
        self.assertEqual(res, {b'k1': b'v1-x'})

        res = self.c.mreplace({'k3': 'v3-y', 'k4': 'v4-y'})
        self.assertEqual(res, {b'k3': b'v3-x', b'k4': b'v4-x'})

        # Set NX.
        self.assertEqual(self.c.msetnx({'k2': 'v2', 'k4': 'v4'}), 1)
        self.assertEqual(self.c.msetnx({'k2': 'v2', 'k3': 'v3'}), 0)
        self.assertEqual(self.c.mget(['k1', 'k2', 'k3', 'k4', 'k5']), {
            b'k1': b'v1-z',
            b'k2': b'v2',
            b'k3': b'v3-y',
            b'k4': b'v4-y'})

    def test_bulk_operations_dupsort(self):
        # We need to specify the DB that supports dupsort.
        self.c.use(3)

        self.assertEqual(self.c.mset({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}), 3)
        self.assertEqual(self.c.mset({
            'k1': 'v1-x',
            'k2': 'v2',
            'k4': 'v4',
            'k5': 'v5'}), 3)  # Only k1, k4 and k5 are counted: k2 is same.
        self.assertEqual(self.c.mset({'k5': 'v5-x'}), 1)

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
        self.c.mset({'k5': 'v5-a'})
        self.c.mset({'k5': 'v5-b'})
        self.assertEqual(self.c.mpop(['k1', 'k5']),
                         {b'k1': b'v1', b'k5': b'v5-a'})
        self.assertEqual(self.c.mpop(['k1', 'k5']),
                         {b'k1': b'v1-x', b'k5': b'v5-b'})
        self.assertEqual(self.c.mpop(['k1', 'k5']), {})

        # Restore the values to k1.
        self.c.set('k1', 'v1-x')

        # Bulk replace, returns the previous value (if it exists).
        res = self.c.mreplace({'k3': 'v3-x', 'k4': 'v4-x', 'k1': 'v1-z'})
        self.assertEqual(res, {b'k1': b'v1-x'})

        # Although we overwrote k1, the original value is not preserved!
        self.assertEqual(self.c.getdup('k1'), [b'v1-z'])
        self.assertEqual(self.c.dupcount('k1'), 1)

        # Verify the original values are returned.
        res = self.c.mreplace({'k3': 'v3-y', 'k4': 'v4-y'})
        self.assertEqual(res, {b'k3': b'v3-x', b'k4': b'v4-x'})

        # Set NX works the same as non-dupsort.
        self.assertEqual(self.c.msetnx({'k2': 'v2', 'k4': 'v4'}), 1)
        self.assertEqual(self.c.msetnx({'k2': 'v2', 'k3': 'v3'}), 0)
        self.assertEqual(self.c.mget(['k1', 'k2', 'k3', 'k4', 'k5']), {
            b'k1': b'v1-z',
            b'k2': b'v2',
            b'k3': b'v3-y',
            b'k4': b'v4-y'})

    def test_getrange(self):
        self.c.mset(dict(('k%s' % i, 'v%s' % i) for i in range(20)))
        sorted_values = list(map(int, sorted(map(str, range(20)))))
        def assertRange(start, end, indices):
            res = self.c.getrange(start, end)
            self.assertEqual(res, [[b'k%s' % i, b'v%s' % i] for i in indices])

        assertRange('k3', 'k6', [3, 4, 5, 6])
        assertRange('k18', 'k3', [18, 19, 2, 3])
        assertRange('k3x', 'k6x', [4, 5, 6])
        assertRange('k0', 'k12', [0, 1, 10, 11, 12])
        assertRange('k01', 'k121', [1, 10, 11, 12])

        # Test boundaries.
        assertRange(None, None, sorted_values)
        assertRange(None, 'kz', sorted_values)
        assertRange('a0', None, sorted_values)
        assertRange('k0', None, sorted_values)
        assertRange('k0', 'k9', sorted_values)

        # Test out-of-bounds.
        assertRange(None, 'a0', [])
        assertRange('z0', None, [])
        assertRange('a0', 'a99', [])
        assertRange('z0', 'z99', [])

    def test_getrange_dupsort(self):
        self.c.use(3)
        nums = [0, 1, 10, 2, 3, 4]
        self.c.mset(dict(('k%s' % i, 'v%s' % i) for i in nums))
        self.c.mset(dict(('k%s' % i, 'v%s-x' % i) for i in nums if i % 2 == 0))

        def assertRange(start, end, indices):
            res = self.c.getrange(start, end)
            accum = []
            for i in indices:
                accum.append([b'k%s' % i, b'v%s' % i])
                if i % 2 == 0:
                    accum.append([b'k%s' % i, b'v%s-x' % i])
            self.assertEqual(res, accum)

        assertRange('k2', 'k4', [2, 3, 4])
        assertRange('k1', 'k3', [1, 10, 2, 3])
        assertRange('k2x', 'k4x', [3, 4])
        assertRange('k0', 'k12', [0, 1, 10])
        assertRange('k01', 'k101', [1, 10])

        # Test boundaries.
        assertRange(None, None, nums)
        assertRange(None, 'kz', nums)
        assertRange('a0', None, nums)
        assertRange('k0', None, nums)
        assertRange('k0', 'k9', nums)

        # Test out-of-bounds.
        assertRange(None, 'a0', [])
        assertRange('z0', None, [])
        assertRange('a0', 'a99', [])
        assertRange('z0', 'z99', [])


if __name__ == '__main__':
    server_t, server, tmp_dir = run_server()
    unittest.main(argv=sys.argv)

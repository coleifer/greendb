#!/usr/bin/env python

import logging
import os
import shutil
import sys
import tempfile
import unittest

import gevent

from clowndb import Client
from clowndb import Server
from clowndb import logger


TEST_HOST = '127.0.0.1'
TEST_PORT = 31327


def run_server():
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.ERROR)
    tmp_dir = tempfile.mkdtemp(suffix='clowndb')
    data_dir = os.path.join(tmp_dir, 'data')
    config_filename = os.path.join(tmp_dir, 'config.json')
    server = Server(host=TEST_HOST, port=TEST_PORT, path=data_dir,
                    config=config_filename)
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


class TestBasicOperations(BaseTestCase):
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


if __name__ == '__main__':
    server_t, server, tmp_dir = run_server()
    unittest.main(argv=sys.argv)

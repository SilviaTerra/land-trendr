from nose.tools import raises
import os
import shutil
import unittest

import utils

class UtilsDecompressTestCase(unittest.TestCase):
    
    @raises(Exception)
    def test_decompress_existing_dir(self):
        utils.decompress('test_files/dummy.csv', '/tmp/')

    @raises(ValueError)
    def test_decompress_invalid_type(self):
        utils.decompress('test_files/dummy.csv', '/tmp/test_invalid_type')
    
    def test_decompress_tar(self):
        test_dir = '/tmp/test_tar'
        files = utils.decompress('test_files/dummy.tar.gz', test_dir)
        self.assertEquals(files, [os.path.join(test_dir, 'dummy.csv')])
        shutil.rmtree(test_dir)

    def test_decompress_zip(self):
        test_dir = '/tmp/test_zip'
        files = utils.decompress('test_files/dummy.zip', test_dir)
        self.assertEquals(files, [os.path.join(test_dir, 'dummy.csv')])
        shutil.rmtree(test_dir)

class Rast2CSVTestCase(unittest.TestCase):
    
    @raises(RuntimeError)
    def test_invalid_type(self):
        utils.rast2csv('test_files/dummy.csv')

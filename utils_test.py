from datetime import datetime
import os
import shutil
import unittest

from nose.tools import raises

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

class DateTestCase(unittest.TestCase):
    @raises(ValueError)
    def test_invalid_date(self):
        utils.parse_date('200-01-13')

    def test_valid_date(self):
        self.assertEquals(utils.parse_date('2012-01-13'), datetime(2012,1,13))

class SerializeRastTestCase(unittest.TestCase):

    @raises(RuntimeError)
    def test_invalid_type(self):
        utils.serialize_rast('test_files/dummy.csv').next()
    
    def test_no_extra_data(self):
        self.assertEquals(
            utils.serialize_rast('test_files/dummy_single_band.tif').next(),
            (
                'POINT(-2097378.06273 2642045.53514)',
                {'val': 16000.0}
            )
        )

    def test_extra_data(self):
        extra = {'date': "2013-01-30"}
        self.assertEquals(
            utils.serialize_rast('test_files/dummy_single_band.tif', extra).next(),
            (
                'POINT(-2097378.06273 2642045.53514)',
                {'date': '2013-01-30', 'val': 16000.0}
            )
        )

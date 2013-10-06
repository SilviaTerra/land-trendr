from datetime import datetime
import os
import shutil
import unittest

from nose.tools import raises
from osgeo import gdal

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

class ParseEqnTestCase(unittest.TestCase):

    def test_no_match(self):
        self.assertEquals([], utils.parse_eqn_bands(''))
        self.assertEquals([], utils.parse_eqn_bands('12 - 4'))

    def test_match(self):
        self.assertEquals([1], utils.parse_eqn_bands('B1'))
        self.assertEquals(
            set([2,3,4,6]), 
            set(utils.parse_eqn_bands('(B2-B2)/(B3+B4)-B6'))
        )

class RastUtilsTestCase(unittest.TestCase):
    def rast2array2rast(self):
        template_fn = 'test_files/dummy_single_band.tif'
        ds = gdal.Open(template_fn)
        array = utils.ds2array(ds)
        self.assertEquals(array.shape, (45, 54))
        rast_fn = utils.array2raster(array, template_fn)
        self.assertEqual(rast_fn, '/tmp/dummy_single_band.tif')
        os.remove(rast_fn)

    @raises(Exception)
    def array2raster_invalid_dim(self):
        template_fn = 'test_files/dummy_single_band.tif'
        ds = gdal.Open(template_fn)
        array = utils.ds2array(ds)
        utils.array2raster(array.transpose(), template_fn)

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

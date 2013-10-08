from datetime import datetime
import numpy
import os
import pandas as pd
import shutil
import unittest

from nose.tools import raises
from osgeo import gdal

import utils

class UtilsDecompressTestCase(unittest.TestCase):
    
    @raises(Exception)
    def test_decompress_existing_dir(self):
        utils.decompress('tests/files/dummy.csv', '/tmp/')

    @raises(ValueError)
    def test_decompress_invalid_type(self):
        utils.decompress('tests/files/dummy.csv', '/tmp/test_invalid_type')
    
    def test_decompress_tar(self):
        test_dir = '/tmp/test_tar'
        files = utils.decompress('tests/files/dummy.tar.gz', test_dir)
        self.assertEquals(files, [os.path.join(test_dir, 'dummy.csv')])
        shutil.rmtree(test_dir)

    def test_decompress_zip(self):
        test_dir = '/tmp/test_zip'
        files = utils.decompress('tests/files/dummy.zip', test_dir)
        self.assertEquals(files, [os.path.join(test_dir, 'dummy.csv')])
        shutil.rmtree(test_dir)

class DateTestCase(unittest.TestCase):
    
    @raises(ValueError)
    def test_invalid_date(self):
        utils.parse_date('200-01-13')

    def test_valid_date(self):
        self.assertEquals(utils.parse_date('2012-01-13'), datetime(2012,1,13))

    def test_filename2date(self):
        self.assertEquals(utils.filename2date('/tmp/4529_2012_01_15.txt'), '2012-01-15')

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

class MultipleReplaceTestCase(unittest.TestCase):

    def test_replace(self):
        replacements = {'X': '3', 'Y': '2', 'Z': '1'}
        self.assertEquals(
            utils.multiple_replace('(X + Y) / (X-Y) = Z', replacements),
            '(3 + 2) / (3-2) = 1'
        )

class SerializeRastTestCase(unittest.TestCase):
    
    @raises(RuntimeError)
    def test_invalid_type(self):
        utils.serialize_rast('tests/files/dummy.csv').next()
    
    def test_no_extra_data(self):
        self.assertEquals(
            utils.serialize_rast('tests/files/dummy_single_band.tif').next(),
            (
                'POINT(-2097378.06273 2642045.53514)',
                {'val': 16000.0}
            )
        )

    def test_extra_data(self):
        extra = {'date': "2013-01-30"}
        self.assertEquals(
            utils.serialize_rast('tests/files/dummy_single_band.tif', extra).next(),
            (
                'POINT(-2097378.06273 2642045.53514)',
                {'date': '2013-01-30', 'val': 16000.0}
            )
        )

class RastUtilsTestCase(unittest.TestCase):

    def setUp(self):
        self.template_fn = 'tests/files/dummy_single_band.tif'
    
    def test_rast2array2rast(self):
        ds = gdal.Open(self.template_fn)
        array = utils.ds2array(ds)
        self.assertEquals(array.shape, (45, 54))
        rast_fn = utils.array2raster(array, self.template_fn)
        self.assertEqual(rast_fn, '/tmp/output_dummy_single_band.tif')
        os.remove(rast_fn)

    @raises(Exception)
    def test_array2raster_invalid_dim(self):
        ds = gdal.Open(self.template_fn)
        array = utils.ds2array(ds)
        utils.array2raster(array.transpose(), self.template_fn)

    def test_map_algebra(self):
        alg_fn = utils.rast_algebra(self.template_fn, 'B1/2')
        self.assertEquals(
            numpy.sum(utils.ds2array(gdal.Open(self.template_fn))) / 2,
            numpy.sum(utils.ds2array(gdal.Open(alg_fn)))
        )
        os.remove(alg_fn)

class AnalysisTestCase(unittest.TestCase):

    def spike_helper(self, l1, l2):
        dates = pd.date_range('1/1/2010', periods=len(l1), freq='A')
        s1, s2 = [pd.Series(data=x, index=dates) for x in [l1, l2]]
        numpy.testing.assert_array_equal(utils.despike(s1), s2)

    def test_timeseries2int_series(self):
        data=[1,2,3,4,5]
        ts = pd.Series(
            data=data, 
            index=pd.date_range('1/1/2010', periods=5, freq='A')
        )
        int_series = utils.timeseries2int_series(ts)
        numpy.testing.assert_array_equal(
            int_series,
            pd.Series(data=data, index=[0, 365, 731, 1096, 1461])
        )

    def test_dict2timeseries(self):
        data = [
            {'date': '2012-09-01', 'val': 10.0},
            {'date': '2011-09-01', 'val': 5.0}
        ]
        series = utils.dict2timeseries(data)
        self.assertTrue(numpy.array_equal(series.values, [5.0, 10.0]))

    def test_despike(self):
        self.spike_helper( 
            [1,1,1,5,1,1,1],
            [1,1,1,None,1,1,1]
        )

        self.spike_helper(
            [1,3,1,5,1,1,1],
            [1, None, 1, None, 1, 1, 1]
        )

    def test_least_squares(self):
        self.assertEquals(
           utils.least_squares(pd.Series([1,2.1, 3, 4.4, 4.7])),
           ((0.96999999999999997, 1.1000000000000008), 0.24300000000000019)
        )

    def test_analyze_simple(self):
        line_cost = 2
        values = [
            {'date': '2010-12-31', 'val': 1},
            {'date': '2011-12-31', 'val': 2},
            {'date': '2012-12-31', 'val': 3},
            {'date': '2013-12-31', 'val': 4},
            {'date': '2014-12-31', 'val': 5},
            {'date': '2015-12-31', 'val': 7},
            {'date': '2016-12-31', 'val': 9},
            {'date': '2017-12-31', 'val': 11},
            {'date': '2018-12-31', 'val': 13},
            {'date': '2019-12-31', 'val': 15}
        ]
        expected_out = [
            {'date': '2010-12-31', 'vertex': True, 'fitted_val': 0, 'val': 1, 'spike': False},
            {'date': '2011-12-31', 'vertex': False, 'fitted_val': 0, 'val': 2, 'spike': False},
            {'date': '2012-12-31', 'vertex': False, 'fitted_val': 0, 'val': 3, 'spike': False},
            {'date': '2013-12-31', 'vertex': False, 'fitted_val': 0, 'val': 4, 'spike': False},
            {'date': '2014-12-31', 'vertex': True, 'fitted_val': 0, 'val': 5, 'spike': False},
            {'date': '2015-12-31', 'vertex': False, 'fitted_val': 0, 'val': 7, 'spike': False},
            {'date': '2016-12-31', 'vertex': False, 'fitted_val': 0, 'val': 9, 'spike': False},
            {'date': '2017-12-31', 'vertex': False, 'fitted_val': 0, 'val': 11, 'spike': False},
            {'date': '2018-12-31', 'vertex': False, 'fitted_val': 0, 'val': 13, 'spike': False},
            {'date': '2019-12-31', 'vertex': True, 'fitted_val': 0, 'val': 15, 'spike': False}
        ]
        self.assertEquals(utils.analyze(values, line_cost), expected_out)

    def test_analyze_simple_spike(self):
        line_cost = 2
        values = [
            {'date': '2010-12-31', 'val': 1},
            {'date': '2011-12-31', 'val': 2},
            {'date': '2012-12-31', 'val': 3},
            {'date': '2013-12-31', 'val': 4},
            {'date': '2014-12-31', 'val': 1000},
            {'date': '2015-12-31', 'val': 7},
            {'date': '2016-12-31', 'val': 9},
            {'date': '2017-12-31', 'val': 11},
            {'date': '2018-12-31', 'val': 13},
            {'date': '2019-12-31', 'val': 15}
        ]
        expected_out = [
            {'date': '2010-12-31', 'vertex': True, 'fitted_val': 0, 'val': 1, 'spike': False},
            {'date': '2011-12-31', 'vertex': False, 'fitted_val': 0, 'val': 2, 'spike': False},
            {'date': '2012-12-31', 'vertex': False, 'fitted_val': 0, 'val': 3, 'spike': False},
            {'date': '2013-12-31', 'vertex': False, 'fitted_val': 0, 'val': 4, 'spike': False},
            {'date': '2014-12-31', 'vertex': False, 'fitted_val': 0, 'val': 1000, 'spike': True},
            {'date': '2015-12-31', 'vertex': False, 'fitted_val': 0, 'val': 7, 'spike': False},
            {'date': '2016-12-31', 'vertex': False, 'fitted_val': 0, 'val': 9, 'spike': False},
            {'date': '2017-12-31', 'vertex': False, 'fitted_val': 0, 'val': 11, 'spike': False},
            {'date': '2018-12-31', 'vertex': False, 'fitted_val': 0, 'val': 13, 'spike': False},
            {'date': '2019-12-31', 'vertex': True, 'fitted_val': 0, 'val': 15, 'spike': False}
        ]
        self.assertEquals(utils.analyze(values, line_cost), expected_out)


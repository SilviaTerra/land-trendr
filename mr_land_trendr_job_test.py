import os
import unittest

from mr_land_trendr_job import MRLandTrendrJob

class MRLandTrendrJobTestCase(unittest.TestCase):
    def test_parse_mapper(self):
        job = MRLandTrendrJob('')

        for key, value in job.parse_mapper(None, 'alpha-silviaterra-landtrendr\ttesting/test_raster.zip'):
            self.assertTrue(key)
            self.assertEquals(len(value), 2)
            self.assertTrue('date' in value)
            self.assertTrue('val' in value)

    def test_point_reducer(self):
        job = MRLandTrendrJob('')

        for key, value in job.point_reducer('POINT(-2097378.06273 2642045.53514)', [{'date': '2012-09-01', 'val': 16000.0}, {'date': '1994-09-01', 'val': 32000.0}]):
            self.assertEquals(key, 'POINT(-2097378.06273 2642045.53514)')
            self.assertEquals(value, [{'date': '1994-09-01', 'val': 32000.0}, {'date': '2012-09-01', 'val': 16000.0}])

    def test_steps(self):
        job = MRLandTrendrJob('')
        self.assertEquals(len(job.steps()), 1)
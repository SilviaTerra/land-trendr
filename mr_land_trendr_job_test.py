import os
import unittest

from mr_land_trendr_job import MRLandTrendrJob

class MRLandTrendrJobTestCase(unittest.TestCase):
    def test_parse_mapper(self):
        job = MRLandTrendrJob()

        for key, value in job.parse_mapper(None, 'alpha-silviaterra-landtrendr\ttesting/ledaps_clipped.zip'):
            self.assertTrue(os.path.exists(key))
            self.assertTrue(os.path.isfile(key))
            self.assertTrue(os.path.getsize(key) > 0)

            os.remove(key)

    def test_steps(self):
        job = MRLandTrendrJob()
        self.assertEquals(len(job.steps()), 1)
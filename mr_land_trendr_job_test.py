import unittest

from mr_land_trendr_job import MRLandTrendrJob

class MRLandTrendrJobTestCase(unittest.TestCase):
    def test_steps(self):
        job = MRLandTrendrJob()
        self.assertEquals(len(job.steps()), 1)

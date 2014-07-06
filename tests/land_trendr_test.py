import boto
import os
import unittest

from land_trendr import create_input_file

class LandTrendrTestCase(unittest.TestCase):
    def test_local_create_input_file(self):
        input_file = create_input_file('local', 'test-job')

        self.assertTrue(os.path.exists(input_file))
        self.assertTrue(os.path.isfile(input_file))

        o = open(input_file)
        contents = o.read()
        o.close()

        self.assertTrue(contents.strip())

        os.remove(input_file)

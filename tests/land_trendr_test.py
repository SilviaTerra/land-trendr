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

    def test_s3_create_input_file(self):
        input_file = create_input_file('emr', 'test-job')
        self.assertEquals(input_file,
            's3://alpha-silviaterra-landtrendr/test-job/input/emr_input.txt')


        # TODO
        #connection = boto.connect_s3()
        #s3_bucket = connection.get_bucket()
        #s3_key = s3_bucket.get_key(key)

        #contents = s3_key.get_contents_as_string()

        #self.assertTrue(contents.strip())

        #s3_key.delete()

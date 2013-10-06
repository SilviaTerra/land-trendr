import boto
import os
import unittest

from land_trendr import create_input_file, S3_REGEX

class LandTrendrTestCase(unittest.TestCase):
    def test_local_create_input_file(self):
        input_file = create_input_file('local', 'silviaterra-hacks', 'bu_landsats')

        self.assertTrue(os.path.exists(input_file))
        self.assertTrue(os.path.isfile(input_file))

        o = open(input_file)
        contents = o.read()
        o.close()

        self.assertTrue(contents.strip())

        os.remove(input_file)

    def test_s3_create_input_file(self):
        input_file = create_input_file('emr', 'silviaterra-hacks', 'bu_landsats')

        match = S3_REGEX.match(input_file)

        self.assertTrue(match is not None)

        bucket, key = match.groups()

        connection = boto.connect_s3()
        s3_bucket = connection.get_bucket(bucket)
        s3_key = s3_bucket.get_key(key)

        contents = s3_key.get_contents_as_string()

        self.assertTrue(contents.strip())

        s3_key.delete()
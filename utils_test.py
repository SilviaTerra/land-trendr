from nose.tools import raises
import unittest

import utils

class UtilsTestCase(unittest.TestCase):
    
    @raises(ValueError)
    def test_decompress_invalid_type(self):
        utils.decompress('test_files/dummy.csv')
        

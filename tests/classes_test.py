import unittest

from nose.tools import raises

import classes

class LabelRuleTestCase(unittest.TestCase):

    def test_create(self):
        lr = classes.LabelRule('greatest_fast_disturbance', 5, 'GD', 
                          duration=['<', 4])
        self.assertEqual(lr.name, 'greatest_fast_disturbance')
        self.assertEqual(lr.val, 5)
        self.assertEqual(lr.change_type, 'GD')
        self.assertEqual(lr.duration, ['<', 4])

    @raises(ValueError)
    def test_invalid_create(self):
        classes.LabelRule('greatest_fast_disturbance', 5, 'GD', 
                          duration=['<', 4, 'BAD'])

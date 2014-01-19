import unittest

from nose.tools import raises

import classes, utils


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



class TrendLineTestCase(unittest.TestCase):

    def test_match(self):
        line_cost = 2
        values = [
            {'date': '2010-12-31', 'val': 10},
            {'date': '2011-12-31', 'val': 10},
            {'date': '2012-12-31', 'val': 10},
            {'date': '2013-12-31', 'val': 5},
            {'date': '2014-12-31', 'val': 5},
            {'date': '2015-12-31', 'val': 5},
            {'date': '2016-12-31', 'val': 7},
            {'date': '2017-12-31', 'val': 9},
            {'date': '2018-12-31', 'val': 10},
            {'date': '2019-12-31', 'val': 10}
        ]
        rule = classes.LabelRule('fast_dist', 2, 'GD', duration=['<', 4])
        trendline = utils.analyze(values, line_cost)
        match = trendline.match_rule(rule)
        self.assertTrue(match is not None)
        self.assertEqual(match.onset_year, 2010)
        self.assertAlmostEqual(match.initial_val, 10.999178383)
        self.assertAlmostEqual(match.magnitude, 6.3993420469846)
        self.assertEqual(match.duration, 3)
        


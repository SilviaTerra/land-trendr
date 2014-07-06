import unittest

from nose.tools import raises

import classes
import utils


class LabelRuleTestCase(unittest.TestCase):

    def test_create(self):
        lr = classes.LabelRule({
            'name': 'greatest_fast_disturbance',
            'val': 5,
            'change_type': 'GD',
            'duration': ['<', 4]
        })
        self.assertEqual(lr.name, 'greatest_fast_disturbance')
        self.assertEqual(lr.val, 5)
        self.assertEqual(lr.change_type, 'GD')
        self.assertEqual(lr.duration, ['<', 4])

    @raises(ValueError)
    def test_invalid_create(self):
        classes.LabelRule({
            'name': 'greatest_fast_disturbance',
            'val': 5,
            'change_type': 'GD',
            'duration': ['<', 4, 'BAD']
        })


class TrendLineTestCase(unittest.TestCase):

    def test_match(self):
        line_cost = 2
        target_date = utils.parse_date('2014-07-01')
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
        rule = classes.LabelRule({
            'name': 'fast_dist',
            'val': 2,
            'change_type': 'GD',
            'duration': ['<', 4]
        })
        trendline = utils.analyze(values, line_cost, target_date)
        match = trendline.match_rule(rule)
        self.assertTrue(match is not None)
        self.assertEqual(match.onset_year, 2010)
        self.assertAlmostEqual(match.initial_val, 10.999999999)
        self.assertAlmostEqual(match.magnitude, 6.3999999999999)
        self.assertEqual(match.duration, 3)

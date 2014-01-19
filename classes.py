class LabelRule:
    """
    Rules for labeling a trendline.

    name and val are the name and integer code for the label

    change_type is the type of disturbance we're looking for.  Options are:
        FD - first disturbance
        GD - greatest disturbance
        LD - longest disturbance
        None - no rule

    all other arguments are two-item lists in the format (qualifier, val).

    onset_year lets us limit the time horizon we analyze.  Qualifier options:
        = - equal to
        >= - greater than or equal to
        <= - less than or equal to

    duration lets us filter by how long a disturbance is.  Qualifier options:
        > - greater than
        < - less than

    pre_threshold lets us filter by the pre-disturbance value.
    Qualifier options:
        > - greater than
        < - less than
    """

    def __init__(self, name, val, change_type=None,
                 onset_year=None, duration=None, pre_threshold=None):

        if not name:
            raise ValueError('name required')
        self.name = name

        if not val:
            raise ValueError('val required')
        self.val = val

        if change_type not in ['FD', 'GD', 'LD', None]:
            raise ValueError('Invalid change_type: %s' % change_type)
        self.change_type = change_type

        for param_name, param_val in zip(
                ['onset_year', 'duration', 'pre_threshold'],
                [onset_year, duration, pre_threshold]):

            if param_val:
                if type(param_val) != list or len(param_val) != 2:
                    raise ValueError('Parameter %s - invalid value: %s' % (
                        param_name, param_val
                    ))
                else:  # TODO more checking for valid codes
                    setattr(self, param_name, param_val)
            else:
                setattr(self, param_name, None)


class TrendlinePoint:
    """
    Represents a single point in a trendline.  Contains lots of data
    about the value of the point, the fitted line going through it,
    and whether or not it's a spike and/or vertex.
    """
    def __init__(self, val_raw, val_fit, eqn_fit, eqn_right, index_date,
                 index_day, spike, vertex):
        self.val_raw = val_raw
        self.val_fit = val_fit
        self.eqn_fit = eqn_fit
        self.eqn_right = eqn_right
        self.index_date = index_date
        self.index_day = index_day
        self.spike = spike
        self.vertex = vertex


class Trendline:
    """
    Represents a processed LandTrendr trendline
    """
    def __init__(self, points):
        self.points = points

    def __unicode__(self):
        vertices = filter(lambda x: x.vertex, self.points)
        return '\n'.join([
            ' | '.join([v.index_date, str(v.val_fit)]) for v in vertices
        ])

    def __str__(self):
        return unicode(self).encode('utf-8')

    def parse_disturbances(self):
        """
        For a given trendline, for each segment between vertices, determine
        the stats for each "disturbance" and return a list of Disturbance
        objects
        """
        import utils
        it = iter(self.points)
        left_vertex = it.next()
        for p in it:
            if not p.vertex:
                continue
            start_yr = utils.parse_date(left_vertex.index_date).year
            end_yr = utils.parse_date(p.index_date).year
            yield Disturbance(
                start_yr,
                left_vertex.val_fit,
                left_vertex.val_fit - p.val_fit,  # TODO %?
                end_yr - start_yr
            )
            left_vertex = p

    def match_rule(self, rule):
        """
        Given a LabelRule, determine if any of the disturbances in this
        trendline match it.  If so, return the disturbance.
        """
        disturbances = self.parse_disturbances()

        # Filter by year_onset, duration, and pre_disturbance_threshold
        matching_disturbances = []
        for d in disturbances:
            match = True

            if rule.onset_year:
                qualifier, yr = rule.onset_year
                if qualifier == '=' and d.onset_year != yr:
                    match = False
                elif qualifier == '<=' and d.onset_year > yr:
                    match = False
                elif qualifier == '>=' and d.onset_year < yr:
                    match = False

            if rule.duration:
                qualifier, yr_length = rule.duration
                if qualifier == '>' and d.duration <= yr_length:
                    match = False
                elif qualifier == '<' and d.duration >= yr_length:
                    match = False

            if rule.pre_threshold:
                qualifier, threshold = rule.threshold
                if qualifier == '>' and d.initial_val <= threshold:
                    match = False
                elif qualifier == '<' and d.initial_val >= threshold:
                    match = False

            if match:
                matching_disturbances.append(d)

        # Pick winner by change type
        winner = None
        for d in matching_disturbances:
            if winner is None:
                winner = d
            else:
                if rule.change_type == 'FD':
                    if d.onset_year < winner.onset_year:
                        winner = d
                elif rule.change_type == 'GD':
                    if d.magnitude > winner.magnitude:
                        winner = d
                elif rule.change_type == 'LD':
                    if d.duration > winner.duration:
                        winner = d

        return winner


class Disturbance:
    """
    Represents a disturance with an onset_year, initial_val, magnitude, and
    duration
    """
    def __init__(self, onset_year, initial_val, magnitude, duration):
        self.onset_year = onset_year
        self.initial_val = initial_val
        self.magnitude = magnitude
        self.duration = duration

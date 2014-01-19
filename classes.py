

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

    pre_threshold lets us filter by the pre-disturbance value.  Qualifier options:
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
                    raise ValueError('Parameter %s passed invalid value: %s' % (
                        param_name, param_val
                    ))
                else:  # TODO more checking for valid codes
                    setattr(self, param_name, param_val)
            else:
                setattr(self, param_name, None)


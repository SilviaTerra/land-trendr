import settings as s

###############################
# Compression / Decompression
###############################
import os
import shutil
import tarfile
import zipfile

def compress(fns, out_fn='/tmp/compressed.zip'):
    """
    Given a list of files
    """
    zf = zipfile.ZipFile(out_fn, 'w')
    for fn in fns:
        zf.write(zf, os.pat.basename(fn), zipfile.ZIP_DEFLATED)
    zf.close()
    return out_fn

def decompress(filename, out_dir='/tmp/decompressed'):
    """
    Given a tar.gz or a zip, extract the contents and return a list of files.
    The out_dir must not already exist.  It will be created from scratch and
    filled with the files from the compressed file
    """
    fn = filename #alias

    if os.path.exists(out_dir):
        raise Exception('Output directory already exists')
    os.makedirs(out_dir)
    del_dir = False

    try:
        if zipfile.is_zipfile(fn):
            zipfile.ZipFile(fn, 'r').extractall(out_dir)
        elif tarfile.is_tarfile(fn):
            tarfile.open(fn, 'r').extractall(out_dir)
        else:
            raise ValueError('Invalid file type - must be tar.gz or zip')
    except Exception as e:
        del_dir = True #delete the partially created out_dir
        raise e #pass exception through
    finally:
        if del_dir:
            shutil.rmtree(out_dir)
    
    return [os.path.join(out_dir, f) for f in os.listdir(out_dir)]


#######
# AWS 
#######
import boto
import json

def keyname2filename(keyname):
    """
    Given a keyname, convert it to a filename on the local machine.

    Note: just returns a string, doesn't actually download the file
    """
    return os.path.join(
        s.WORK_DIR, keyname.replace(os.path.sep, '__')
    )


def get_keys(prefix):
    """
    returns the keys of all S3 objects with that prefix
    or [] if there are no such S3 objects
    """
    conn = boto.connect_s3()
    bucket = conn.get_bucket(s.S3_BUCKET)
    for k in bucket.list(prefix=prefix):
        if k.key.endswith('/'):
            continue  # skip directories
        yield k

def download(keys):
    filenames = []

    for key in keys:
        filename = keyname2filename(key.key)
        key.get_contents_to_filename(filename)
        filenames.append(filename)

    return filenames

def get_files(prefix):
    """
    Given an S3 prefix,
    download and the files if they don't already exist
    and return the filenames
    """
    to_download = []
    fns = []
    for k in get_keys(prefix):
        fn = keyname2filename(k.key)
        fns.append(fn)
        if not os.path.exists(fn):
            to_download.append(k)

    download(to_download)
    return fns


def get_file(keyname):
    """
    Gets a single file from S3 and returns the local filename.
    Throws an error if more than one match or zero matches.
    """
    fn, to_dl = None, None
    i = -1
    for i, k in enumerate(get_keys(keyname)):
        if i == 1:
            raise Exception('More than one key matches prefix "%s"' % keyname)
        fn = keyname2filename(k.key)
        if not os.path.exists(fn):
            to_dl = k
    if i == -1:
        raise Exception('No key matches prefix "%s"' % keyname)

    if to_dl:
        download([to_dl])
    return fn

def read_json(keyname):
    """
    Read a JSON file from S3 and return the python object
    """
    keys = list(get_keys(keyname))
    num_keys = len(keys)
    if num_keys == 1:
        key = keys[0]
        return json.loads(key.get_contents_as_string())
    elif num_keys == 0:
        raise Exception('File with key %s does not exist' % keyname)
    else:
        raise Exception('More than one file with prefix %s' % keyname)

def upload(filenames, replacements={}):
    """
    Uploads a list of files to S3.  Converts "__" into "/"
    """
    keys = []
    conn = boto.connect_s3()
    bucket = conn.get_bucket(s.S3_BUCKET)

    replacements.update({
        '__': '/',
        os.path.join(s.WORK_DIR, ''):  ''
    })

    for filename in filenames:
        keyname = multiple_replace(filename, replacements)
        key = bucket.new_key(keyname)
        key.set_contents_from_filename(filename)
        keys.append(key)

    return keys


#################################
# String parsing
#################################
from datetime import datetime
import re 

def parse_date(date_string):
    """
    Given a date string in the format YYYY-MM-DD, return a date
    Raise an error if invalid format
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except Exception:
        raise ValueError('date_string must be in "YYYY-MM-DD" format')

def filename2date(fn):
    """
    Given a filename, returns a datestring in the format YYYY-MM-DD
    """
    fn, _ = os.path.splitext(fn)
    scene_id, y, m, d = os.path.basename(fn).split('_')
    return '%s-%s-%s' % (y, m, d)

def parse_eqn_bands(eqn):
    """
    Given a string equation like:
        '(B3-B2)/(B3+B2)'
    Return the numbers for the bands (e.g. [2,3])
    """
    return [int(d) for d in set(re.compile('B(?P<band_num>\d+)').findall(eqn))]

def multiple_replace(string, replacements):
    """
    Given a string and a dictionary of replacements in the format:
        { <word_to_replace>: <replacement>, ... }
    Make all the replacements and return the new string.

    From: http://stackoverflow.com/questions/2400504/
    """
    pattern = re.compile('|'.join(replacements.keys()))
    return pattern.sub(lambda x: replacements[x.group()], string)

####################
# Raster Read/Write
####################
import numpy as np  
from osgeo import gdal

NODATA = -99  # TODO settings

### READ ###

def get_pix_offsets_for_point(ds, lng, lat):
    """
    Given a raster datasource (from osgeo.gdal)
    and lng/lat coordinates, return the x and y
    pixel offsets required to get the pixel that
    point is in.
    
    Note - this assumes that the point is within the datasource bounds
    """
    top_left_x, pix_width, _, top_left_y, _, pix_height = ds.GetGeoTransform()
    
    x_distance = lng - top_left_x
    x_offset = int(x_distance * 1.0 / pix_width)
    
    y_distance = lat - top_left_y
    y_offset = int(y_distance * 1.0 / pix_height) #pix_height is negative
    
    return x_offset, y_offset

def ds2array(ds, band=1):
    """
    Given a datasource and optionally a band number, 
    return the array of values
    """
    num_pix_wide, num_pix_high = ds.RasterXSize, ds.RasterYSize
    if band > ds.RasterCount:
        raise Exception('Band %s requested but raster only has %s bands' % (
            band, ds.RasterCount
        ))
    return ds.GetRasterBand(band).ReadAsArray(0, 0, num_pix_wide, num_pix_high)

def serialize_rast(rast_fn, extra_data={}):
    """
    Given a georeferenced raster filename,
    and optionally a dictionary of extra data to include in each output,
    returns an iterator that generates lines in the format:
        "<pt_wkt>", {'val':<val>, <extra_key1>:<extra_val1>, ...}
    """
    gdal.UseExceptions()  # enable exception-throwing by GDAL

    ds = gdal.Open(rast_fn)
    num_pix_wide, num_pix_high = ds.RasterXSize, ds.RasterYSize
    top_left_x, pix_width, x_rot, top_left_y, y_rot, pix_height = ds.GetGeoTransform()
    
    band = ds.GetRasterBand(1)
    pixvals = band.ReadAsArray(0, 0, num_pix_wide, num_pix_high)
    
    for xoff in xrange(num_pix_wide):
        x = top_left_x + (xoff + 0.5) * pix_width # +0.5 to get center x
        for yoff in xrange(num_pix_high):
            y = top_left_y + (yoff + 0.5) * pix_height # +0.5 to get center y
            val = pixvals[yoff, xoff] # careful!  math matrix uses yoff, xoff
            pt_wkt = 'POINT(%s %s)' % (x, y)
            pt_data = {'val': float(val)}
            pt_data.update(extra_data)
            yield pt_wkt, pt_data

### WRITE ###

def array2raster(array, template_rast_fn, out_fn=None, data_type=None):
    """
    Given a 2-dimensional np array and a template raster,
    write the array out to a georeferenced raster in the same style as the template.

    For the no_data_val and data_type, if no value is specified it falls back
    to whatever those settings are in the template
    """
    if not out_fn:
        out_fn = os.path.join('/tmp', 'output_%s' % os.path.basename(template_rast_fn))

    template_ds = gdal.Open(template_rast_fn)
    ds_shape = (template_ds.RasterYSize, template_ds.RasterXSize)
    if array.shape != ds_shape:
        raise Exception(
            'Dimensions of array %s and template raster %s don\'t match' % (
                array.shape, ds_shape
            )
        )
    if not data_type:
        data_type = template_ds.GetRasterBand(1).DataType

    driver = template_ds.GetDriver()
    out_ds = driver.Create(
        out_fn, template_ds.RasterXSize, template_ds.RasterYSize, 1, data_type
    )
    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(NODATA)
    out_band.WriteArray(array, 0, 0)
    
    # georeference image
    out_ds.SetGeoTransform(template_ds.GetGeoTransform())
    out_ds.SetProjection(template_ds.GetProjection())

    return out_fn

def data2raster(data, template_fn, out_fn='/tmp/rast.tif'):
    """
    Given an iterable (list) in the format:
        {'pix_ctr_wkt': wkt, 'value': val}
    And a tif to use as a template,
    Create a new tif where the values are
    filled in to the raster.
    
    Returns the new filename
    
    Note: NODATA value hardcoded as -99
    """
    template_ds = gdal.Open(template_fn)

    # initialize array to all NODATA
    holder = np.ones_like(ds2array(template_ds)) * NODATA
    
    for d in data:
        clean = d['pix_ctr_wkt'].replace('POINT(', '').replace(')', '').strip()
        lng, lat = clean.split(' ')
        val = float(d['value'])
        
        # figure out where the pixel goes
        x_off, y_off = get_pix_offsets_for_point(template_ds, lng, lat)
        holder[y_off, x_off] = val  # careful!  matrix uses y, x notation
   
    return array2raster(holder, template_fn, out_fn)


##################
# Raster algebra
##################
# numpy referenced in eval code
def rast_algebra(rast_fn, eqn, mask_eqn=None, out_fn='/tmp/rast_algebra.tif'):
    """
    Given a raster file, 
    a string equation in the format: TODO,
    and an optional output file name,

    create a new raster with the equation applied to it
    """
    gdal.UseExceptions() # enable exception-throwing by GDAL
    
    ds = gdal.Open(rast_fn)

    eqn_bands = parse_eqn_bands(eqn)
    mask_bands = parse_eqn_bands(mask_eqn or '')
    all_bands = set(eqn_bands + mask_bands)

    min_band, max_band = min(all_bands), max(all_bands)
    if max_band > ds.RasterCount:
        raise Exception('Band %s not present in %s' % (max_band, rast_fn))
    if min_band <= 0:
        raise Exception('Invalid band "%s" - bands must be >= 1')

    # bands is referenced in the modified equations
    bands = dict([(b, ds2array(ds, b)) for b in all_bands])
    
    band_replace = dict([('B%s' %b, 'bands[%s]' % b) for b in all_bands])
    mod_eqn = multiple_replace(eqn, band_replace)

    if mask_eqn:
        mod_mask_eqn = multiple_replace(mask_eqn, band_replace)
        no_data_val = NODATA  # referenced in modified equation below
        data = eval(
            'np.choose(%s, (no_data_val, %s)))' % (mod_mask_eqn, mod_eqn)
        )
    else:
        data = eval(mod_eqn)

    return array2raster(data, rast_fn, out_fn=out_fn)

#############
# Analysis
#############
import pandas as pd

def timeseries2int_series(time_series):
    """
    It's tricky to do math on dates, so instead of having dates 
    as the index for a series, start the series at 0 and instead 
    use the number of days since the beginning of the series as the index.

    Returns a new series
    """
    dates = time_series.index.values
    days_series = [
        (d - dates[0]).astype('timedelta64[D]').item().days for d in dates
    ]
    return pd.Series(data=time_series.values, index=days_series)

def dicts2timeseries(dict_list):
    """
    Given a list of dicts in the format:
        [{'date': '2011-09-01', 'val': 160.0}, {'date': '2012-09-01', 'val': 160.0}]
    Put them into a sorted time series and return
    """
    dates = [parse_date(x['date']) for x in dict_list]
    vals = [x['val'] for x in dict_list]
    series = pd.Series(data=vals, index=dates)
    return series.sort_index()

def despike(time_series):
    """
    Given a sorted timeseries, remove any spikes.
    
    Right now the algorithm is very simple, it just finds the standard 
    deviation and prunes out any points that are more than a standard 
    deviation away.

    Outputs a modified series with nulled out outliers
    """
    std_dev = np.std(time_series) # standard deviation
    triples = [time_series[i:i+3] for i in range(0, len(time_series)-2)]
    despiked = []
    despiked.append(time_series[0]) # first not an outlier
    last_good = time_series[0]
    for x, y, z in triples:
        # If x and z are on same side of y (both above or both below)
        # and both x and z are more than a std_dev away...
        if not ((x <= y <= z) or (x >= y >= z)) and \
                ((abs(y-x) > std_dev) and (abs(y-z) > std_dev)) and \
                y != last_good:
            despiked.append(None)
        else:
            despiked.append(y)
            last_good = y
    despiked.append(time_series[-1]) # last not an outlier
    return pd.Series(data=despiked, index=time_series.index.values)

def least_squares(series):
    """
    Given a series, calculate the line:
        y = mx + c
    That minimizes the sums of squared errors through the points.
    Return:
        (m, c), sum_residuals
    """
    series = series.dropna() # clean out any NaN vals
    x, y = series.index.values, series.values 
    A = np.vstack([x, np.ones(len(x))]).T
    solution = np.linalg.lstsq(A, y)
    m, c = solution[0]
    sum_residuals = np.sum(solution[1])
    return (m, c), sum_residuals

def segmented_least_squares(series, line_cost):
    """
    Given a series, use Bellman's dynamic programming segmented least squares
    algorithm to find the set of lines that minimizes the sum of:
        * total sums of squared errors
        * num_lines * line_cost
    Return the series indices of the endpoints of the lines
    """
    series = series.dropna() # remove any NaN vals
    n = len(series)
    
    e = dict([(i, {}) for i in range(n)]) # errors (residuals) 
    # format: {
    #    <start_x>: {<end_x>: <resid>, <end_x+1>: <resid>, ...}, 
    #    ...
    # }
    
    # calculate least squares between all points
    for j in range(0, n):
        for i in range(0, j+1):
            e[i][j] = 0 if i == j else least_squares(series[i:j+1])[1]

    # calculate optimal cost for each step
    OPT = {-1: 0}
    for j in range(0, n):
        vals = [(e[i][j] + line_cost + OPT[i-1]) for i in range(0, j+1)]
        OPT[j] = min(vals)

    # unfurl the optimal segments backwards to find the segments
    list_indices = find_segments(n-1, e, line_cost, OPT) 
    list_indices += [n-1] # last index always in
    return [series.index.values[x] for x in list_indices]

def find_segments(j, e, c, OPT):
    """
    Given an index j, a residuals dictionary, a line cost, and a
    dictionary of optimal costs for each index,
    return a list of the optimal endpoints for least squares segments from 0-j
    """
    if j == -1:
        return []
    else:
        vals = [(e[i][j] + c + OPT[i-1]) for i in range(0, j+1)]
        min_index = vals.index(min(vals))
        return find_segments(min_index-1, e, c, OPT) + [min_index]

def vertices2eqns(series, is_vertex):
    """
    Given a series and an equally long boolean array of whether or not each 
    item in the series is a vertex, calculate the regression segments and
    return 2 new series 
    (each is in reference to the regression line on the right of each item):
        m: the slope
        b: the y-intercept
    """
    vertices = series[is_vertex]
    v_idx = vertices.index
    vertex_pairs = [v_idx[i:i+2] for i in range(len(vertices)-1)]
    vertex_eqns = dict([ # format {<vertex_label>: <(m,c)>, ...}
        (v1, least_squares(series.loc[v1:v2])[0]) for v1, v2 in vertex_pairs
    ])
    # set last vertex eqn to the second to last vertex eqn
    vertex_eqns[v_idx[-1]] = vertex_eqns[v_idx[-2]] 

    eqns, curr_eqn = [], None
    for i, is_v in zip(series.index, is_vertex):
        if is_v:
            curr_eqn = vertex_eqns[i] or curr_eqn # defaults to curr_eqn if last vertex
        eqns.append(curr_eqn)
    return pd.Series(data=eqns, index=series.index)

def apply_eqn(x, eqn):
    """
    Given a value "x" and an equation tuple in the format:
        (m, b)
    where m is the slope and b is the y-intercept,
    return the "y" generated by:
        y = mx + b
    """
    m, b = eqn 
    return (m * x) + b

def eqns2fitted_points(series, equations):
    """
    Given data in a series as well as a series of equations describing the 
    least squares regression line (to the right of each point in the series),
    calculate the fitted value at each point in the series.

    Returns two series:
        fit_pts - a series of the fitted points 
        fit_eqns - a series of the equation used to fit each point

    NOTE: when a point has differing equations on the right and left 
    (occurs at vertex points), choose whichever results in the smallest 
    residual
    """
    fit_pts, fit_eqns = [], []
    left_eqn = None # no left eqn for first point
    for raw_val, idx, right_eqn in zip(series.values, series.index, equations):
        if (not left_eqn) or (left_eqn == right_eqn):
            fit_val = apply_eqn(idx, right_eqn)
            fit_eqn = right_eqn
        else:
            fit_left = apply_eqn(idx, left_eqn)
            fit_right = apply_eqn(idx, right_eqn)
            resid_left = abs(fit_left - raw_val)
            resid_right = abs(fit_right - raw_val)
            if resid_left <= resid_right:
                fit_val = fit_left 
                fit_eqn = left_eqn
            else:
                fit_val = fit_right
                fit_eqn = right_eqn
        
        fit_pts.append(fit_val)
        fit_eqns.append(fit_eqn)
        
        left_eqn = right_eqn

    return (
        pd.Series(fit_pts, index=series.index), 
        pd.Series(fit_eqns, index=series.index)
    )

def get_idx(array_like, idx):
    """
    Given an array-like object (either list or series),
    return the value at the requested index
    """
    if hasattr(array_like, 'iloc'):
        return array_like.iloc[idx]
    else:
        return array_like[idx]

from classes import Trendline, TrendlinePoint
def analyze(pix_datas, line_cost):
    """
    Given data in the format:
    [
        {'date': '2011-09-01', 'val': 160.0}, 
        {'date': '2012-09-01', 'val': 180.0},
        ...
    ]
    Run a bunch of analysis on it to do change labeling

    Returns a Trendline
    """
    # convert to time series 
    ts = dicts2timeseries(pix_datas)
    formatted_dates = [d.strftime('%Y-%m-%d') for d in ts.index]

    # despike
    despiked = despike(ts)
    is_spike = pd.isnull(despiked)

    # convert from time series to int series (for least squares)
    int_series = timeseries2int_series(despiked)

    # get vertices
    vertices = segmented_least_squares(int_series, line_cost)
    is_vertex = [x in vertices for x in int_series.index]

    # get least squares regression equations at each point
    eqns_right = vertices2eqns(int_series, is_vertex)

    # calculate fitted values
    vals_fit, eqns_fit = eqns2fitted_points(int_series, eqns_right)

    outs = {
        'val_raw': ts.values, 
        'val_fit': vals_fit, 
        'eqn_fit': eqns_fit,
        'eqn_right': eqns_right,
        'index_date': formatted_dates, 
        'index_day': int_series.index, 
        'spike': is_spike, 
        'vertex': is_vertex
    }
    trendline_points = []
    for i in range(ts.size):
        kwargs = dict([
            (key, get_idx(series, i)) 
            for key, series in outs.iteritems()
        ])
        trendline_points.append(TrendlinePoint(**kwargs))
    
    return Trendline(trendline_points)


#######################
### Change Labeling ###
#######################
def change_labeling(pix_trendline, label_rules):
    """
    Given a Trendline and a list of LabelRules

    For each label, determine if the pixel matches.  If so, output:
    {
        <label_name>: {
            'class_val': X,
            'onset_year': X,
            'magnitude': X, 
            'duration': X
        }, ...
    }
    """
    labels = {}
    for rule in label_rules:
        print 'Checking rule %s' % rule.name
        match = pix_trendline.match_rule(rule)
        if match:
            labels[rule.name] = {
                'class_val': rule.val,
                'onset_year': match.onset_year,
                'magnitude': match.magnitude, 
                'duration': match.duration
            }

    return labels

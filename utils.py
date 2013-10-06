import os
import shutil
import tarfile
import zipfile

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
    
    return [os.path.join(out_dir, fn) for fn in os.listdir(out_dir)]

from datetime import datetime

def parse_date(date_string):
    """
    Given a date string in the format YYYY-MM-DD, return a date
    Raise an error if invalid format
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except Exception:
        raise ValueError('date_string must be in "YYYY-MM-DD" format')

import re
def parse_eqn_bands(eqn):
    """
    Given a string equation like:
        '(B3-B2)/(B3+B2)'
    Return the numbers for the bands (e.g. [2,3])
    """
    return [int(d) for d in set(re.compile('B(?P<band_num>\d+)').findall(eqn))]

import numpy
from osgeo import gdal

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

def array2raster(array, template_rast_fn, out_fn=None, no_data_val=None, data_type=None):
    """
    Given a 2-dimensional numpy array and a template raster,
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

    if no_data_val:
        no_data_val = template_ds.GetRasterBand(1).GetNoDataValue()
    if no_data_val:
        out_band.SetNoDataValue(no_data_val)
    
    out_band.WriteArray(array, 0, 0)
    
    #georeference image
    out_ds.SetGeoTransform(template_ds.GetGeoTransform())
    out_ds.SetProjection(template_ds.GetProjection())

    return out_fn

def rast_algebra(rast_fn, eqn, mask_eqn=None, no_data_val=None, out_fn='/tmp/rast_algebra.tif'):
    """
    Given a raster file, 
    a string equation in the format: TODO,
    and an optional output file name,

    create a new raster with the equation applied to it
    """
    gdal.UseExceptions() #enable exception-throwing by GDAL
    
    ds = gdal.Open(rast_fn)
    if not no_data_val:
        no_data_val = ds.GetRasterBand(1).GetNoDataValue()

    eqn_bands = parse_eqn_bands(eqn)
    mask_bands = parse_eqn_bands(mask_eqn or '')
    all_bands = eqn_bands.union(mask_bands)

    min_band, max_band = min(all_bands), max(all_bands)
    if max_band > ds.RasterCount:
        raise Exception('Band %s not present in %s' % (max_band, rast_fn))
    if min_band <= 0:
        raise Exception('Invalid band "%s" - bands must be >= 1')

    eqn = '(B3-B2)/(B3+B2)'

    band_dict = dict([(b_num, ds2array(ds, b_num)) for b_num in all_bands])
    

    mod_eqn, mod_mask_eqn = eqn, mask_eqn or ''
    if band_dict:
        for e in [mod_eqn, mod_mask_eqn]:
            for b in all_bands:
                e = e.replace('B%s' % b, 'band_dict[%s]' % b)

    if mask_eqn:
        data = eval(
            'numpy.choose(%s, (no_data_val, %s)))' % (mod_mask_eqn, mod_eqn)
        )
    else:
        data = eval(mod_eqn)

def serialize_rast(rast_fn, extra_data={}):
    """
    Given a georeferenced raster filename,
    and optionally a dictionary of extra data to include in each output,
    returns an iterator that generates lines in the format:
        "<pt_wkt>", {'val':<val>, <extra_key1>:<extra_val1>, ...}
    """
    gdal.UseExceptions() #enable exception-throwing by GDAL

    ds = gdal.Open(rast_fn)
    num_pix_wide, num_pix_high = ds.RasterXSize, ds.RasterYSize
    top_left_x, pix_width, x_rot, top_left_y, y_rot, pix_height = ds.GetGeoTransform()
    
    band = ds.GetRasterBand(1)
    pixvals = band.ReadAsArray(0, 0, num_pix_wide, num_pix_high)
    
    for xoff in xrange(num_pix_wide):
        x = top_left_x + (xoff + 0.5) * pix_width # +0.5 to get center x
        for yoff in xrange(num_pix_high):
            y = top_left_y + (yoff + 0.5) * pix_height # +0.5 to get center y
            val = pixvals[yoff, xoff] #careful!  math matrix uses yoff, xoff
            pt_wkt = 'POINT(%s %s)' % (x, y)
            pt_data = {'val': float(val)}
            pt_data.update(extra_data)
            yield pt_wkt, pt_data


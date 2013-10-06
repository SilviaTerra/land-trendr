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
from osgeo import gdal
import simplejson

def ds2array(ds, band=1):
    """
    Given a datasource and optionally a band number, 
    return the array of values
    """
    num_pix_wide, num_pix_high = ds.RasterXSize, ds.RasterYSize
    return ds.GetRasterBand(band).ReadAsArray(0, 0, num_pix_wide, num_pix_high)

def rast_algebra(rast_fn, eqn, out_fn='/tmp/rast_algebra.tif'):
    """
    Given a raster file, 
    a string equation in the format: TODO,
    and an optional output file name,

    create a new raster with the equation applied to it
    """
    gdal.UseExceptions() #enable exception-throwing by GDAL
    
    ds = gdal.Open(rast_fn)
    num_bands = ds.RasterCount

    eqn = '(B3-B2)/(B3+B2)'


    bands = [1,2,3]
    band_dict = dict([(b_num, ds2array(ds, b_num)) for b_num in bands])
    

#data2 = band2.ReadAsArray(0, 0, cols, rows).astype(Numeric.Float16)
#data3 = band3.ReadAsArray(0, 0, cols, rows).astype(Numeric.Float16)
#mask = Numeric.greater(data3 + data2, 0)
#ndvi = Numeric.choose(mask, (-99, (data3 - data2) / (data3 + data2)))

def parse_date(date_string):
    """
    Given a date string in the format YYYY-MM-DD, return a date
    Raise an error if invalid format
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%d')
    except Exception:
        raise ValueError('date_string must be in "YYYY-MM-DD" format')

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


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

def rast2text(rast_fn, date_string):
    """
    Given a georeferenced raster filename, 
    returns an iterator that generates lines in the format:
        "<pt_wkt>", "{'date': '<YYYY-MM-DD>', 'val':<val>}"
    """
    gdal.UseExceptions() #enable exception-throwing by GDAL
    
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
    except Exception:
        raise Exception('date_string must be in "YYYY-MM-DD" format')

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
            pt_data = simplejson.dumps({'date': date_string, 'val': val})
            yield pt_wkt, pt_data


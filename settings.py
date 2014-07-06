import os

INPUT_FILE = 'input.txt'

WORK_DIR = '/mnt/vol'

if os.environ.get('LANDTRENDR_TESTING') == 'True':
    S3_BUCKET = 'alpha-silviaterra-landtrendr'
else:
    S3_BUCKET = 'silviaterra-landtrendr'

# if these strings appear in filename, classify as raster or mask
RAST_TRIGGER = 'ledaps'  # to be used for analysis
MASK_TRIGGER = 'cloudmask'  # to be used for masking

NODATA = -99  # nodata value for rasters

IN_EMR_KEYNAME = '%s/input/emr_input.txt'  # % job
IN_SETTINGS = '%s/input/settings.json'  # % job
IN_RASTS = '%s/input/rasters/'  # % job


OUT_GRID = '%s/output/pix_grid.csv'  # % job
OUT_RAST_KEYNAME = '%s/output/rasters/%s.tif'  # % (job, label)

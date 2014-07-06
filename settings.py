import os

INPUT_FILE = 'input.txt'

WORK_DIR = '/mnt/vol'

if os.environ.get('LANDTRENDR_TESTING') == 'True':
    S3_BUCKET = 'alpha-silviaterra-landtrendr'
else:
    S3_BUCKET = 'silviaterra-landtrendr'

# TODO clarify that this can be anywhere, not just a suffix
RAST_SUFFIX = 'ledaps'  # to be used for analysis
MASK_SUFFIX = 'cloudmask'  # to be used for masking

IN_EMR_KEYNAME = '%s/input/emr_input.txt'  # % job
IN_SETTINGS = '%s/input/settings.json'  # % job
IN_RASTS = '%s/input/rasters/'  # % job


OUT_GRID = '%s/output/pix_grid.csv'  # % job
OUT_RAST_KEYNAME = '%s/output/rasters/%s.tif'  # % (job, label)

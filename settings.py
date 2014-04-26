import os

INPUT_FILE = 'input.txt'

WORK_DIR = '/mnt/vol'

if os.environ['LANDTRENDR_TESTING'] == 'True':
    S3_BUCKET = 'alpha-silviaterra-landtrendr'
else:
    S3_BUCKET = 'silviaterra-landtrendr'

IN_EMR_KEYNAME = '%s/input/emr_input.txt'  # % job
IN_SETTINGS = '%s/input/settings.json'  # % job
IN_RASTS = '%s/input/rasters/'  # % job


OUT_GRID = '%s/output/pix_grid.csv'  # % job
OUT_RAST_KEYNAME = '%s/output/rasters/%s.tif'  # % (job, label)

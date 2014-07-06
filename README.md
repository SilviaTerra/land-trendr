land-trendr
===========
Mapreduce implementation of Dr. Robert Kennedy's LandTrendr change detection system

 * Original project website: http://landtrendr.forestry.oregonstate.edu/
 * Original project code: https://github.com/KennedyResearch/LandTrendr-2012

Install
-------
Note: you must put your AWS credentials in the ~/.boto file as described here:
http://boto.readthedocs.org/en/latest/boto_config_tut.html

    sudo mkdir -p /mnt/vol && sudo chmod -R a+rwx /mnt/vol # for storing rasters
    sudo apt-get install python-pip python-virtualenv virtualenvwrapper
    mkvirtualenv land-trendr
    pip install -r requirements.txt

If you're having trouble installing pip/virtualenv on OSX, check [here](http://jamie.curle.io/blog/installing-pip-virtualenv-and-virtualenvwrapper-on-os-x/)

Problems installing numpy/pandas on OSX?  Check [here](http://stackoverflow.com/questions/22388519/problems-with-pip-install-numpy-runtimeerror-broken-toolchain-cannot-link-a)
  * More details [here](http://kaspermunck.github.io/2014/03/fixing-clang-error/)

If you're getting errors importing gdal/osgeo, try:

    # Ubuntu
    ln -s /usr/lib/python2.7/dist-packages/osgeo $VIRTUAL_ENV/lib/python2.7/site-packages/osgeo

    # OSX
    ln -s /Library/Frameworks/GDAL.framework/Versions/1.10/Python/2.7/site-packages/osgeo $VIRTUAL_ENV/lib/python2.7/site-packages/.

    # or
    pip install --no-install gdal
    cd $VIRTUAL_ENV/build/gdal
    python setup.py build_ext --library-dirs=/Library/Frameworks/GDAL.framework/Versions/Current/unix/lib --include-dirs=/Library/Frameworks/GDAL.framework/Versions/Current/unix/include install

Setting up a job
----------------
In your land-trendr S3 bucket (settings.S3_BUCKET), create the following:
 * __JOB__/input/rasters/...
   * .tar.gz or .zip for each raster you want to include in the analysis
   * names should be in the format: LE7045029_1999_211_20120124_104859_cloudmask_cropped.tif.tar.gz
   * make sure your filenames include the appropriate trigger string
      * see RAST_TRIGGER and MASK_TRIGGER in settings
      * note that mask files are optional
      * note that this isn't really a suffix, it just has to appear somewhere in the filename
 * __JOB__/input/settings.json  -  JSON with the following fields:
   * index_eqn - what equation to use to calculate a single index from a multi-band image
     * e.g. "(B1+B2)/(B3-B2)"
   * line_cost - the cost of adding a line in the segmented least squares algorithm
   * target_date - when choosing between multiple images in a year, the target_date
        is used to pick the closest image.  The year is automatically changed for each set of images.
        Format: YYYY-MM-DD
   * label_rules - a list of change-labeling rules in the format:
     * name - name of the change label
     * val - what value to use when writing to raster
     * change_type - one of:
        * FD - first disturbance
        * GD - greatest disturbance
        * LD - longest disturbance
     * The following OPTIONAL options all take two-item lists in the format (qualifier, val):
        * onset_year lets us limit the time horizon we analyze. Qualifier options:
          * = - equal to
          * >= - greater than or equal to
          * <= - less than or equal to
        * duration lets us filter by how long a disturbance is. Qualifier options:
          * > - greater than
          * < - less than
        * pre_threshold lets us filter by the pre-disturbance value. Qualifier options:
          * > - greater than
          * < - less than

Example settings.json
---------------------
    {
        "index_eqn": "B1 - B2",
        "line_cost": 10,
        "target_date": "2014-07-01",
        "label_rules": [
            {
                "name": "greatest_disturbance",
                "val": 1,
                "change_type": "GD"
            }
        ]
    }

Output
------
The output is uploaded in internally compressed (LZW) tif files to the
    __job__/output/rasters/
folder.  For each change label, there is a class, duration, magnitude, and onset_year tif.

There is also a "trendline" folder that has an exhaustive set of the trendline
variables for each year.

Running locally
---------------
    python land_trendr.py -p local -j __JOB__

Running on EMR
--------------
    python land_trendr.py -p emr -j __JOB__

Running tests
-------------
    ./run_tests.sh

Overall Architecture
--------------------
The main flow-control part of this program is located in mr_land_trendr_job.py.
Note that mapper/reducer in this context refer to the MapReduce paradigm.
There are 5 major steps:
 1. setup_mapper - creates a  grid of points to sample all the rasters by.
    * input - nothing
    * output - list of raw rasters analyze
 2. parse_mapper - calculates an index for each raster and samples by each point in the grid (masking appropriately)
    * input - single raster S3 keyname
    * output - image date and raster value for each sample, keyed on grid point WKT 
 3. analysis_reducer - aggregates all values for each point in the grid, calculates trendline and change labels, and outputs the change labels for each point
    * input - all dates/values for each grid point
    * output - change labels for each point
 4. label_mapper - just bookkeeping, reaggregates by label rather than by grid point WKT
    * input - change labels keyed by grid point WKT
    * output - grid points and labels keyed by label type (class_val, onset_year, magnitude, duration)
 5. output_reducer - writes output to raster files and uploads to S3
    * input - all the pixel/label data for a certain label type
    * output - s3 keyname of uploaded raster

How the analysis works
----------------------
The main analysis flow-control function is utils.analyze.  
It computes a trendline and is comprised of the following steps:
 1. For each year, pick a winning pixel
    * Chooses the median pixel if multiple exist
 2. Convert the list into a time series (for analysis with pandas)
 3. Despike the series
 4. Perform the segmented-least-squares algorithm to identify vertices
 5. Given vertices, determine the equation of the least-squares line at each point
 6. Calculate the fitted value for each point
 7. Returns a classes.Trendline object

How the change labeling works
-----------------------------
You can specify multiple change labeling rules in the settings.json file.
Each label is checked against the Trendline object to see if it matches.
See the Trendline.parse_disturbances and Trendline.match_rule functions for
specifics of the implementation.

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
   * make sure your filenames include the appropriate suffix
      * see RAST_SUFFIX and MASK_SUFFIX in settings
      * note that mask files are optional
      * note that this isn't really a suffix, it just has to appear somewhere in the filename
 * __JOB__/input/settings.json  -  JSON with the following fields:
   * index_eqn - what equation to use to calculate a single index from a multi-band image
     * e.g. "(B1+B2)/(B3-B2)"
   * line_cost - the cost of adding a line in the segmented least squares algorithm
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
        "index_eqn": "B1",
        "line_cost": 10,
        "label_rules": [
            {
                "name": "greatest_disturbance",
                "val": 1,
                "change_type": "GD"
            }
        ]
    }


Running locally
---------------
    python land_trendr.py -p local -j __JOB__

Running on EMR
--------------
    python land_trendr.py -p emr -j __JOB__

Running tests
-------------
    ./run_tests.sh


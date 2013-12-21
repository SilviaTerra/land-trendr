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


Running locally
---------------
    python land_trendr.py -p local -i s3://alpha-silviaterra-landtrendr/testing/

Running on EMR
--------------
    python land_trendr.py -p emr -i s3://alpha-silviaterra-landtrendr/testing/ -o s3://alpha-silviaterra-landtrendr/testing/output/

Running tests
-------------
    ./run_tests.sh


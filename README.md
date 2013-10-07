land-trendr
===========
Mapreduce implementation of Dr. Robert Kennedy's LandTrendr change detection system

 * Original project website: http://landtrendr.forestry.oregonstate.edu/
 * Original project code: https://github.com/KennedyResearch/LandTrendr-2012

Install
-------
    sudo apt-get install python-pip python-virtualenv virtualenvwrapper
    pip install -r requirements.txt

If you're getting errors importing gdal, try:

    ln -s /usr/lib/python2.7/dist-packages/osgeo $VIRTUAL_ENV/lib/python2.7/site-packages/osgeo

Running locally
---------------
    python land_trendr.py -p local -i s3://alpha-silviaterra-landtrendr/testing

Running tests
-------------
    nosetest

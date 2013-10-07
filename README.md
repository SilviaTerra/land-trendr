land-trendr
===========

Map reduce implementation of Dr. Robert Kennedy's LandTrendr change detection system

Running tests
-------------
    nosetest --with-coverage

Install
-------
If you're getting errors importing gdal, try:

    ln -s /usr/lib/python2.7/dist-packages/osgeo $VIRTUAL_ENV/lib/python2.7/site-packages/osgeo

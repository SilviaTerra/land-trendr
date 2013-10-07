land-trendr
===========
Mapreduce implementation of Dr. Robert Kennedy's LandTrendr change detection system

Install
-------
    sudo apt-get install python-pip python-virtualenv virtualenvwrapper
    pip install -r requirements.txt

If you're getting errors importing gdal, try:

    ln -s /usr/lib/python2.7/dist-packages/osgeo $VIRTUAL_ENV/lib/python2.7/site-packages/osgeo

Running tests
-------------
    nosetest

nosetests \
	--with-coverage \
	--cover-min-percentage=75 \
	--cover-package=utils \
	--cover-erase \
	--cover-html \
	--cover-html-dir=/tmp/land_trendr_coverage
echo 'coverage HTML at /tmp/land_trendr_coverage'

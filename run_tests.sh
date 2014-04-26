test_env_backup=$LANDTRENDR_TESTING
job_env_backup=$LT_JOB
export LANDTRENDR_TESTING=True
export LT_JOB="test-job"
nosetests \
	--with-coverage \
	--cover-min-percentage=75 \
	--cover-package=utils \
	--cover-erase \
	--cover-html \
	--cover-html-dir=/tmp/land_trendr_coverage
echo 'coverage HTML at /tmp/land_trendr_coverage'
export LANDTRENDR_TESTING=$test_env_backup
export LT_JOB=$job_env_backup

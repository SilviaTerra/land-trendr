import argparse
import boto
import tarfile

import settings as s
from mr_land_trendr_job import MRLandTrendrJob

DEPENDENCIES_TARFILE = '/tmp/landtrendr_dependencies.tar.gz'
DEPENDENCIES = [
    'settings.py',
    'classes.py',
    'utils.py'
]

DEFAULT_EMR_JOB_RUNNER_KWARGS = {
    'enable_emr_debugging': True,
    'no_output': True,
    'bootstrap_cmds': [
        'sudo apt-get -y install gdal-bin python-gdal python-pip',
        'sudo pip install boto numpy pandas'
    ],
    'python_archives': [DEPENDENCIES_TARFILE]
}


def add_bootstrap_cmds():
    connection = boto.connect_s3()

    DEFAULT_EMR_JOB_RUNNER_KWARGS['bootstrap_cmds'] += [
        'echo [Credentials] | sudo tee /etc/boto.cfg',
        'echo aws_access_key_id = %s | sudo tee -a /etc/boto.cfg' % connection.access_key,
        'echo aws_secret_access_key = %s | sudo tee -a /etc/boto.cfg' % connection.secret_key
    ]


def bundle_dependencies():
    tar = tarfile.open(DEPENDENCIES_TARFILE, 'w:gz')

    for fn in DEPENDENCIES:
        tar.add(fn)

    tar.close()


def create_input_file(platform, job):
    # just a dummy value.  All settings pulled from S3
    contents = 'Running-%s' % job

    if platform == 'local':
        local_input = '/tmp/input.txt'
        o = open(local_input, 'w')
        o.write(contents)
        o.close()

        return local_input

    if platform == 'emr':
        connection = boto.connect_s3()
        bucket = connection.get_bucket(s.S3_BUCKET)
        key = bucket.new_key(s.IN_EMR_KEYNAME % job)
        key.set_contents_from_string(contents)

        return 's3://%s/%s' % (s.S3_BUCKET, key.key)


def main(platform, job):
    args, job_runner_kwargs = [], {}
    job_runner_kwargs['input_paths'] = [create_input_file(platform, job)]

    if platform == 'emr':
        args = ['-r', 'emr']
        add_bootstrap_cmds()
        emr_job_runner_kwargs = DEFAULT_EMR_JOB_RUNNER_KWARGS
        bundle_dependencies()
    else:
        emr_job_runner_kwargs = {}

    job = MRLandTrendrJob(
        args=args,
        job_runner_kwargs=job_runner_kwargs,
        emr_job_runner_kwargs=emr_job_runner_kwargs
    )

    with job.make_runner() as runner:
        runner.run()

        if platform != 'emr':
            for line in runner.stream_output():
                print job.parse_output_line(line)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a LandTrendr job')

    parser.add_argument('-p', '--platform', required=True,
                        choices=['local', 'emr'],
                        help='Which platform do you want to run on?')
    parser.add_argument('-j', '--job', required=True,
                        help='Which LandTrendr job do you want to run?')

    args = parser.parse_args()
    main(args.platform, args.job)

import argparse
import boto
import datetime
import os
import re
import tarfile

from mr_land_trendr_job import MRLandTrendrJob

DEPENDENCIES_TARFILE = '/tmp/landtrendr_dependencies.tar.gz'
DEPENDENCIES = []

EMR_DEFAULT_OPTIONS = {
    'enable_emr_debugging': True,
    'no_output': True,
    'bootstrap_cmds': [
        'sudo apt-get -y install gdal-bin python-gdal python-pip',
        'sudo pip install boto numpy'
    ],
    'python_archive': [DEPENDENCIES_TARFILE]
}

S3_REGEX = re.compile('s3://([\w\-]+)/([\w\-\./]+)')
INPUT_FILE = 'input.txt'
LOCAL_INPUT_FILE = '/tmp/%s' % INPUT_FILE

def bundle_dependencies():
    tar = tarfile.open(DEPENDENCIES_TARFILE, 'w:gz')

    for fn in DEPENDENCIES:
        tar.add(fn)

    tar.close()

def create_input_file(platform, input_bucket, input_path):
    connection = boto.connect_s3()
    bucket = connection.get_bucket(input_bucket)

    contents = '\n'.join(['%s\t%s' % (bucket.name, key.key) for key in bucket.list(prefix=input_path) if key.key.endswith('.zip') or key.key.endswith('.tar.gz')])

    if platform == 'local':
        o = open(LOCAL_INPUT_FILE, 'w')
        o.write(contents)
        o.close()

        return LOCAL_INPUT_FILE

    if platform == 'emr':
        input_key = '%s/%s' % (input_path, INPUT_FILE)
    
        key = bucket.new_key(input_key)
        key.set_contents_from_string(contents)
    
        return 's3://%s/%s' % (input_bucket, input_key)

def main(platform, input_bucket, input_path, output=None):
    args, runner_kwargs = [], {}
    runner_kwargs['input_paths'] = [create_input_file(platform, input_bucket, input_path)]

    if platform == 'emr':
        args = ['-r', 'emr'] 
        runner_kwargs.update(EMR_DEFAULT_OPTIONS)
        runner_kwargs['output_dir'] = output
        bundle_dependencies()

    job = MRLandTrendrJob(args=args, runner_kwargs=runner_kwargs)

    with job.make_runner() as runner:
        runner.run()

        if platform == 'local':
            for line in runner.stream_output():
                print job.parse_output_line(line)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a LandTrendr job')

    parser.add_argument('-p', '--platform', required=True, choices=['local', 'emr'], 
        help='Which platform do you want to run on?')
    parser.add_argument('-i', '--input', required=True,
        help='Where to find the input.  s3 path.')
    parser.add_argument('-o', '--output',
        help='Where to save the output.  Only valid and required for EMR')

    args = parser.parse_args()

    if args.platform == 'emr':
        if not args.output:
            raise argparse.ArgumentError('Must specify output s3 path for EMR job')
    elif args.platform == 'local':
        if args.output:
            raise argparse.ArgumentError('Output file not a local arg for local job')

    match = S3_REGEX.match(args.input)

    if match is None:
        raise argparse.ArgumentError('Invalid input')

    input_bucket, input_path = match.groups()

    main(args.platform, input_bucket, input_path, args.output)

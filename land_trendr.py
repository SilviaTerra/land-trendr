import argparse
import os
import tarfile

from mr_land_trendr_job import MRLandTrendrJob

DEPENDENCIES_TARFILE = '/tmp/landtrendr_dependencies.tar.gz'
DEPENDENCIES = []

EMR_DEFAULT_OPTIONS = {
    'enable_emr_debugging': True,
    'no_output': True,
    'bootstrap_cmds': ['sudo apt-get -y install gdal-bin python-gdal'],
    'python_archive': [DEPENDENCIES_TARFILE]
}

def bundle_dependencies():
    tar = tarfile.open(DEPENDENCIES_TARFILE, 'w:gz')

    for fn in DEPENDENCIES:
        tar.add(fn)

    tar.close()

def main(platform, input, output=None):
    args, runner_kwargs = [], {}
    runner_kwargs['input_paths'] = [input]

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
        help='Where to find the input file.  If local, the filepath.  If EMR, s3 path.')
    parser.add_argument('-o', '--output',
        help='Where to save the output.  Only valid and required for EMR')

    args = parser.parse_args()

    if args.platform == 'emr':
        if not args.output:
            raise argparse.ArgumentError('Must specify output s3 path for EMR job')
    elif args.platform == 'local':
        if args.output:
            raise argparse.ArgumentError('Output file not a local arg for local job')

    main(args.platform, args.input, args.output)

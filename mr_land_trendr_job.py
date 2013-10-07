import boto
import os

from mrjob.job import MRJob

DOWNLOAD_DIR = '/tmp'

class MRLandTrendrJob(MRJob):
    def __init__(self, index_eqn, *args, **kwargs):
        runner_kwargs = kwargs.pop('runner_kwargs', {})
        super(MRLandTrendrJob, self).__init__(*args, **kwargs)
        self.index_eqn = index_eqn
        self.runner_kwargs = runner_kwargs

    def parse_mapper(self, _, line):
        s3_bucket, s3_key = line.split('\t')
        connection = boto.connect_s3()
        bucket = connection.get_bucket(s3_bucket)
        key = bucket.get_key(s3_key)
        filename = os.path.join(DOWNLOAD_DIR, os.path.basename(key.key))
        key.get_contents_to_filename(filename)
        # TODO: Max
        yield filename, None

    def mapper(self, _, line):
        point, date, index = line.split('\t')
        yield (point, {'date': date, 'index': index})

    def reducer(self, point, values):
        yield (point, sorted(values, key=lambda value: value['date']))

    def steps(self):
        return [
            self.mr(mapper=self.parse_mapper)
        ]
    
    def job_runner_kwargs(self):
        kwargs = super(MRLandTrendrJob, self).job_runner_kwargs()
        kwargs.update(self.runner_kwargs)        
        return kwargs

if __name__ == '__main__':
    MRLandTrendrJob.run()

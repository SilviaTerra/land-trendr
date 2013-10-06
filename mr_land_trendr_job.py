from mrjob.job import MRJob

class MRLandTrendrJob(MRJob):
    def __init__(self, *args, **kwargs):
        runner_kwargs = kwargs.pop('runner_kwargs', {})
        super(MRLandTrendrJob, self).__init__(*args, **kwargs)
        self.runner_kwargs = runner_kwargs

    def mapper(self, _, line):
        point, date, index = line.split('\t')
        yield (point, {'date': date, 'index': index})

    def reducer(self, point, values):
        yield (point, sorted(values, key=lambda value: value['date']))

    def steps(self):
        return [
            self.mr(mapper=self.mapper,
                    reducer=self.reducer)
        ]
    
    def job_runner_kwargs(self):
        kwargs = super(MRLandTrendrJob, self).job_runner_kwargs()
        kwargs.update(self.runner_kwargs)        
        return kwargs

if __name__ == '__main__':
    MRLandTrendrJob.run()

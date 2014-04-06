import os
import shutil

from mrjob.job import MRJob

import settings as s
import utils
import classes


class MRLandTrendrJob(MRJob):
    def __init__(self, *args, **kwargs):
        lt_job = kwargs.pop('lt_job', None)
        if not lt_job:
            raise Exception('lt_job is a required input')
        extra_job_runner_kwargs = kwargs.pop('job_runner_kwargs', {})
        extra_emr_job_runner_kwargs = kwargs.pop('emr_job_runner_kwargs', {})
        super(MRLandTrendrJob, self).__init__(*args, **kwargs)
        self.lt_job = lt_job
        self.extra_job_runner_kwargs = extra_job_runner_kwargs
        self.extra_emr_job_runner_kwargs = extra_emr_job_runner_kwargs

    def setup_mapper(self, _, line):
        """
        Reads in a dummy line from the input.txt file, ignores it,
        and sets up the job passed to MRLandTrendrJob by reading from
        the input S3 dir for that job.

        Outputs a list of the S3 keys for each of the input rasters
        """
        rast_keys = utils.get_keys(s.IN_RASTS)
        for i, k in enumerate(rast_keys):
            yield i, k.key

    def parse_mapper(self, _, rast_s3key):
        """
        Given a line containing a s3 keyname of a raster,
        download the mentioned file and split it into pixels
        in the format:
            point_wkt, {'val': <val>, 'date': <date>}
        (where the point_wkt is the centroid of the pixel)
        """
        rast_zip_fn = utils.get_file(rast_s3key)
        rast_fn = utils.decompress(rast_zip_fn)[0]  # TODO to a named dir

        index_eqn = utils.get_settings(self.lt_job)['index_eqn']
        index_rast = utils.rast_algebra(rast_fn, index_eqn)

        shutil.rmtree(os.path.dirname(rast_fn))  # TODO this is scary

        datestring = utils.filename2date(rast_fn)
        pix_generator = utils.serialize_rast(index_rast, {'date': datestring})
        for point_wkt, pix_data in pix_generator:
            yield point_wkt, pix_data

    def analysis_reducer(self, point_wkt, pix_datas):
        """
        Given a point wkt and a list of pix datas in the format:
        [
            {'date': '2011-09-01', 'val': 160.0},
            {'date': '2012-09-01', 'val': 180.0},
            ...
        ]
        perform the landtrendr analysis and change labeling.

        Return the point_wkt and a dictionary of "change labels" that match
        the generated trendline.
        """
        settings = utils.get_settings(self.lt_job)
        pix_trendline = list(utils.analyze(pix_datas, settings['line_cost']))

        label_rules = [
            classes.LabelRule(lr) for lr in settings['label_rules']
        ]

        change_labels = utils.change_labeling(pix_trendline, label_rules)
        yield point_wkt, change_labels

    def label_mapper(self, point_wkt, change_labels):
        """
        Given all the change labels and metadata for a particular pixel,
        Split all the pixels by label and type (e.g. onset_year)
        """
        for label_name, data in change_labels.iteritems():
            for key in ['class_val', 'onset_year', 'magnitude', 'duration']:
                label_key = '%s_%s' % (label_name, key)
                yield label_key, {'pix_ctr_wkt': point_wkt, 'value': data[key]}

    def mapping_reducer(self, label_key, pix_datas):
        """
        fill the data in to a raster image and return the
        names of the generated images
        """
        # download a template raster
        template_key = utils.get_keys(s.IN_RASTS)[0]
        template_rast = utils.get_file(template_key)
        
        # name raster so it uploads to correct location
        rast_key = s.OUT_RAST_KEYNAME % (self.lt_job, label_key)
        rast_fn = utils.keyname2filename(rast_key)
        
        # write data to raster
        utils.data2raster(pix_datas, template_rast, out_fn=rast_fn)
        compressed = utils.compress([rast_fn], '%s.zip' % rast_fn)

        # upload raster
        rast_key = utils.upload(compressed)[0]
        yield label_key, [rast_key]

    def steps(self):
        return [
            self.mr(mapper=self.parse_mapper, reducer=self.analysis_reducer),
            self.mr(mapper=self.label_mapper, reducer=self.mapping_reducer)
        ]

    def job_runner_kwargs(self):
        kwargs = super(MRLandTrendrJob, self).job_runner_kwargs()
        kwargs.update(self.extra_job_runner_kwargs)
        return kwargs

    def emr_job_runner_kwargs(self):
        kwargs = super(MRLandTrendrJob, self).emr_job_runner_kwargs()
        kwargs.update(self.extra_emr_job_runner_kwargs)
        return kwargs

if __name__ == '__main__':
    MRLandTrendrJob.run()

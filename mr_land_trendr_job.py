import os
import shutil

from mrjob.job import MRJob

import utils
import classes

CLEANUP = False  # TODO settings


class MRLandTrendrJob(MRJob):
    def __init__(self, *args, **kwargs):
        job = kwargs.pop('job', None)
        if not job:
            raise Exception('job is a required input')
        index_eqn = kwargs.pop('index_eqn', 'B1')
        line_cost = kwargs.pop('line_cost', 10)
        extra_job_runner_kwargs = kwargs.pop('job_runner_kwargs', {})
        extra_emr_job_runner_kwargs = kwargs.pop('emr_job_runner_kwargs', {})
        super(MRLandTrendrJob, self).__init__(*args, **kwargs)
        self.job = job
        self.index_eqn = index_eqn
        self.line_cost = line_cost
        self.extra_job_runner_kwargs = extra_job_runner_kwargs
        self.extra_emr_job_runner_kwargs = extra_emr_job_runner_kwargs

    def parse_mapper(self, _, line):
        """
        Given a line containing s3 bucket/key pairs (tab separated),
        download the mentioned file and split it into pixels 
        in the format:
            point_wkt, {'val': <val>, 'date': <date>}
        (where the point_wkt is the centroid of the pixel)
        """
        s3_bucket, s3_key = line.split('\t')
        rast_zip_fn = utils.get_file(s3_key)
        rast_fn = utils.decompress(rast_zip_fn)[0]
        datestring = utils.filename2date(rast_fn)
        index_rast = utils.rast_algebra(rast_fn, self.index_eqn)
        if CLEANUP:
            shutil.rmtree(os.path.dirname(rast_fn))
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
        pix_trendline = list(utils.analyze(pix_datas, self.line_cost))

        # TODO get change label rules
        label_rules = [
            classes.LabelRule('greatest_fast_disturbance', 3, 'GD'),
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
        RAST_KEYNAME = 'TEMPLATE_RAST.tif'  # TODO dynamic
        template_rast = utils.get_file(RAST_KEYNAME)

        # write data to raster
        RASTER = '%s/output/rasters/%s.tif'  # % (job, label)  # TODO settings
        rast_fn = RASTER % (self.job, label_key)
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

import os
import sys

from mrjob.job import MRJob

import settings as s
import utils
import classes


class MRLandTrendrJob(MRJob):
    def __init__(self, *args, **kwargs):
        extra_job_runner_kwargs = kwargs.pop('job_runner_kwargs', {})
        extra_emr_job_runner_kwargs = kwargs.pop('emr_job_runner_kwargs', {})
        super(MRLandTrendrJob, self).__init__(*args, **kwargs)
        self.extra_job_runner_kwargs = extra_job_runner_kwargs
        self.extra_emr_job_runner_kwargs = extra_emr_job_runner_kwargs

    def setup_mapper(self, _, line):
        """
        Reads in a dummy line from the input.txt file, ignores it,
        and sets up the job passed to MRLandTrendrJob by reading from
        the input S3 dir for that job.

        Outputs a list of the S3 keys for each of the input rasters
        """
        job = os.environ.get('LT_JOB')
        print 'Setting up %s' % job
        rast_keys = [k.key for k in utils.get_keys(s.IN_RASTS % job)]
        analysis_rasts = filter(lambda k: s.RAST_SUFFIX in k, rast_keys)
        if not analysis_rasts:
            raise Exception('No analysis rasters specified for job %s' % job)

        # download template rast for grid
        rast_fn = utils.rast_dl(analysis_rasts[0])

        # set up grid
        grid_fn = utils.keyname2filename(s.OUT_GRID % job)
        utils.rast2grid(rast_fn, out_csv=grid_fn)
        utils.upload([grid_fn])

        # note - must yield at end to ensure grid is created
        for i, keyname in enumerate(analysis_rasts):
            yield i, keyname

    def parse_mapper(self, _, rast_s3key):
        """
        Given a line containing a s3 keyname of a raster,
        download the mentioned file and split it into pixels
        in the format:
            point_wkt, {'val': <val>, 'date': <date>}
        (where the point_wkt is the centroid of the pixel)
        """
        job = os.environ.get('LT_JOB')

        rast_fn = utils.rast_dl(rast_s3key)

        mask_key = rast_s3key.replace(s.RAST_SUFFIX, s.MASK_SUFFIX)
        try:
            mask_fn = utils.rast_dl(mask_key)
        except Exception:
            mask_fn = None  # don't worry about mask

        # calculate index
        index_eqn = utils.get_settings(job)['index_eqn']
        index_rast = utils.rast_algebra(rast_fn, index_eqn)

        # figure out date from filename
        datestring = utils.filename2date(rast_fn)

        # pull down grid
        grid_fn = utils.get_file(s.OUT_GRID % job)

        print 'Serializing %s...' % os.path.basename(rast_fn)
        pix_generator = utils.apply_grid(
            index_rast, grid_fn, {'date': datestring}, mask_fn=mask_fn)

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
        sys.stdout.write('.')  # for viewing progress
        sys.stdout.flush()

        job = os.environ.get('LT_JOB')
        settings = utils.get_settings(job)

        pix_datas = list(pix_datas)  # save iterator to a list
        pix_trendline = utils.analyze(pix_datas, settings['line_cost'])

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

    def output_reducer(self, label_key, pix_datas):
        """
        fill the data in to a raster image and return the
        names of the generated images
        """
        # download a template raster
        job = os.environ.get('LT_JOB')

        rast_keys = utils.get_keys(s.IN_RASTS % job)
        tmplt_key = filter(lambda x: s.RAST_SUFFIX in x.key, rast_keys)[0].key
        tmplt_rast = utils.rast_dl(tmplt_key)

        # name raster so it uploads to correct location
        rast_key = s.OUT_RAST_KEYNAME % (job, label_key)
        rast_fn = utils.keyname2filename(rast_key)

        # write data to raster
        utils.data2raster(pix_datas, tmplt_rast, out_fn=rast_fn)
        compressed = utils.compress([rast_fn], '%s.zip' % rast_fn)

        # upload raster
        rast_key = utils.upload([compressed])[0]
        yield label_key, [rast_key.key]

    def steps(self):
        return [
            self.mr(mapper=self.setup_mapper),
            self.mr(mapper=self.parse_mapper, reducer=self.analysis_reducer),
            self.mr(mapper=self.label_mapper, reducer=self.output_reducer)
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

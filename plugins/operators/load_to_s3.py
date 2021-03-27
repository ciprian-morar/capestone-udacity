import logging
import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import pathlib

# """
# LoadToS3 it's used in the upload
# the data from locally to S3.
# """
class LoadToS3Operator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 aws_conn_id="",
                 bucket_name="",
                 key="",
                 relative_local_path="",
                 region_name="",
                 filename=None,
                 specific_file=False,
                 *args, **kwargs):
        super(LoadToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.key = key
        self.bucket_name = bucket_name
        self.relative_local_path = relative_local_path
        self.region_name=region_name
        self.filename = filename
        self.specific_file = specific_file

    def execute(self, context):
        # connect to redshift with the PostgresHook
        self.log.info('LoadToS3Operator')
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        check_bucket = s3.check_for_bucket(self.bucket_name)
        if check_bucket is not True:
            s3.create_bucket(self.bucket_name, self.region_name)
        self.log.info(pathlib.Path(__file__).parent.absolute())

        # condition which uploading a specific file
        if self.specific_file is True:
            s3.load_file(filename=self.relative_local_path + self.filename,
                         bucket_name=self.bucket_name, replace=True,
                         key=self.key + "/"  + self.filename)
        else:
            #condition which apply to upload more files in a directory
            files = [self.relative_local_path + f
                     for f in os.listdir(self.relative_local_path)]
            #self.log.info(files)
            # files_array = []
            # for subdir, dirs, files in os.walk(self.relative_local_path):
            #     for file in files:
            #         files_array.append(os.path.join(subdir, file))
            for f in files:
                filename = f.split('/')[-1]
                s3.load_file(filename=f, bucket_name=self.bucket_name,
                             replace=True, key=self.key +"/" + filename)
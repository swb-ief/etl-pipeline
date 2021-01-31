import pandas as pd
from backend.repository import Repository

import logging
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

log = logging.getLogger(__name__)


class AWSFileRepository(Repository):
    """ csv file based repository

    :remarks: Assumes AWS credentials are set in the environment as expected by the boto3 library
    Use this in GitHub Workflow (update the secrets names and the region)
    - name: Configure AWS Credentials
      uses: aws-actions/configure-aws-credentials@v1
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: us-east-2

    """

    def __init__(self, bucket):
        self.bucket = bucket

    @staticmethod
    def _convert_to_filename(storage_location):
        return f'{storage_location}.csv'

    def store_dataframe(self, df: pd.DataFrame, storage_location: str, allow_create: bool,
                        store_index: bool = False) -> None:

        file_name = self._convert_to_filename(storage_location)

        if not store_index:
            df = df.reset_index(drop=True)

        df.to_csv(file_name)
        self._upload_file(file_name, self.bucket)

    def get_dataframe(self, storage_location: str) -> pd.DataFrame:

        file_name = self._convert_to_filename(storage_location)

        self._download_file(file_name, self.bucket)
        df = pd.read_csv(file_name)

        return df

    def exists(self, storage_location: str) -> bool:
        """
        :param storage_location:
        :return: True if the location exists
        """
        file_name = self._convert_to_filename(storage_location)
        s3 = boto3.resource('s3')

        # inspired by: https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3
        try:
            s3.Object(self.bucket, file_name).load()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise
        return True

    def create_storage_location(self, storage_location: str) -> None:
        pass  # not needed for AWS

    def delete_storage_location(self, storage_location: str) -> None:
        file_name = self._convert_to_filename(storage_location)
        s3 = boto3.resource('s3')

        try:
            s3.Object(self.bucket, file_name).delete()
        except NoCredentialsError:
            print("Credentials not available")

    def create_repository(self, repository_name: str, admin_email: str) -> str:
        raise NotImplementedError("Not sure if we want to do this for AWS. But this would be bucket creation")

    @staticmethod
    def _upload_file(file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        # expects AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to bet set in the environment
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
        except ClientError as e:
            logging.error(e)
            return False

        return True

    @staticmethod
    def _download_file(file_name, bucket, object_name=None):
        s3 = boto3.client('s3')

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        s3.download_file(bucket, object_name, file_name)

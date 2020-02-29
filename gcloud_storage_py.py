import json
import os
import pandas as pd

# Build Service
from apiclient import discovery
import httplib2
from oauth2client.service_account import ServiceAccountCredentials

# Build Client
from google.cloud import storage

class GCloudStorage(object):

    def __init__(self, service_account_file):
        with open(service_account_file) as f:
            svc_data = dict(json.loads(f.read()))

        self.project_id = svc_data['project_id']
        self.client_email = svc_data['client_email']
        self.client_id = svc_data['client_id']

        # Build Service
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_account_file,
            scopes=['https://www.googleapis.com/auth/devstorage.read_only']
        )
        http = httplib2.Http()
        http = credentials.authorize(http)
        self.storage_service = discovery.build('storage', 'v1', http=http)

        # Build Client
        self.storage_client = storage.Client.from_service_account_json(service_account_file)

    @property
    def storage_client_documentation(self):
        return 'https://pypi.org/project/google-cloud-storage/'

    @property
    def storage_service_documentation(self):
        return 'https://cloud.google.com/storage/docs/json_api/v1'

    ##################
    # CLIENT FUNCTIONS
    ##################

    def get_bucket(self, bucket_name):
        """Returns a bucket object."""
        # bucket_name = "your-bucket-name"
        return self.storage_client.bucket(bucket_name)

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        return destination_file_name

    def get_blop_as_df(self, bucket_name, source_blob_name, destination_file_name, delete_file=False):
        """Returns a blob from the bucket as pandas dataframe."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"

        file_name = self.download_blob(bucket_name, source_blob_name, destination_file_name)
        df = pd.read_csv(file_name)

        if delete_file:
            os.remove(file_name)

        return df

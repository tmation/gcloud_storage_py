import json

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

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

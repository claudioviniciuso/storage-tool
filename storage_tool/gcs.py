from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from storage_tool.base import BaseStorage
from storage_tool.data_processor import DataProcessor


class GCSAuthorization:
    def __init__(self):
        self.credentials = None
        self.project_id = None
        
    def set_credentials(self, project_id, client_id, client_email, private_key, private_key_id):
        credentials_dict = {
            'type': 'service_account',
            'client_id': client_id,
            'client_email': client_email,
            'private_key_id': private_key,
            'private_key': private_key_id,
        }
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            credentials_dict
        )
        self.project_id = project_id
        return "Success, credentials defined"

    def test_credentials(self):
        """
        Test credentials to connect
        """
        try:
            client = storage.Client(credentials=self.auth.credentials, project=self.auth.project_id)
            client.list_buckets()
        except Exception as e:
            print(e)
            return False
        return True
    
class GCSStorage(BaseStorage, DataProcessor):
    # Define permitted return types
    return_types = [str, dict, pd.DataFrame, list]

    def __init__(self, Authorization):
        if not isinstance(Authorization, GCSAuthorization):
            raise Exception('Authorization must be an instance of GCSAuthorization class')
        
        if not Authorization.test_credentials():
            raise Exception('Invalid credentials')

        self.auth = Authorization

    def read(self, file_path):
        """
        Read file
        :param file_path: File path
        return: String File content
        """
        if not self.repository:
            raise Exception('Repository not set')

        try:
            client = storage.Client(credentials=self.auth.credentials, project=self.auth.project_id)
            bucket = client.get_bucket(self.repository)
            blob = bucket.blob(file_path)
            return blob.download_as_string()

        except Exception as e:
            raise Exception(f'Error while reading file: {e}')
    
    def set_repository(self, repository):
        self.repository = repository
        return "Success, {repository} defined".format(repository=repository)
    
    def put(self, file_path, content):
        """
        Write file to GCS
        :param file_path: File path
        :param content: File content

        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            client = storage.Client(credentials=self.auth.credentials, project=self.auth.project_id)
            bucket = client.get_bucket(self.repository)

            bucket.blob(file_path).upload_from_string(content)
            return "Success, file written"

        except Exception as e:
            raise Exception(f'Error while writing file: {e}')
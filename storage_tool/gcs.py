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
            'private_key_id': private_key_id,
            'private_key': private_key,
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
            client = storage.Client(credentials=self.credentials, project=self.project_id)
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
            client = self.get_client()
            bucket = client.get_bucket(self.repository)
            blob = bucket.blob(file_path)
            return blob.download_as_string()

        except Exception as e:
            raise Exception(f'Error while reading file: {e}')
    
    def set_repository(self, repository):
        """
        Verify and set repository
        :param repository: Repository name
        """
        repositories = self.list_repositories()
        exists = any(d["repository"] == repository for d in repositories)
        if not exists:
            raise Exception('Repository not found')

        self.repository = repository
        return "Success, {repository} defined".format(repository=repository)

    def create_repository(self, repository):
        """
        Create repository
        :param repository: Repository name
        """
        client = self.get_client()
        bucket = client.bucket(repository)
        bucket.storage_class = "COLDLINE"
        client.create_bucket(bucket, location="us")

        return "Success, {repository} created".format(repository=repository)
    
    def set_or_create_repository(self, repository):
        """
        Verify and set repository
        :param repository: Repository name
        """
        repositories = self.list_repositories()
        exists = any(d.name == repository for d in repositories)
        if not exists:
            self.create_repository(repository)
        self.repository = repository

        return "Success, {repository} created and defined".format(repository=repository)
    
    def list_repositories(self):
        """
        List all repositories
        """
        client = self.get_client()
        return client.list_buckets()
    
    def get_client(self):
        return storage.Client(credentials=self.auth.credentials, project=self.auth.project_id)
    
    def put(self, file_path, content):
        """
        Write file to GCS
        :param file_path: File path
        :param content: File content

        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            client = self.get_client()
            bucket = client.get_bucket(self.repository)

            bucket.blob(file_path).upload_from_string(content)
            return "Success, file written"

        except Exception as e:
            raise Exception(f'Error while writing file: {e}')
    
    def list(self, path=''):
        """
        List all files and foulders in repository
        :param path: Path to list
        return: List of files and folders
        """
        if not self.repository:
            raise Exception('Repository not set')

        list_files = []
        client = self.get_client()
        blobs = client.list_blobs(self.repository, prefix=path)

        for blob in blobs:
            list_files.append(blob.name)

        return list_files
    
    def delete(self,  file_path):
        """
        Delete file f
        :param file_path: File path

        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            client = self.get_client()
            bucket = client.bucket(self.repository)
            blob = bucket.blob(file_path)
            generation_match_precondition = None

            # Optional: set a generation-match precondition to avoid potential race conditions
            # and data corruptions. The request to delete is aborted if the object's
            # generation number does not match your precondition.
            blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
            generation_match_precondition = blob.generation

            blob.delete(if_generation_match=generation_match_precondition)
            return "Success, file deleted"
        except Exception as e:
            raise Exception(f'Error while deleting file: {e}')
        
    def move_between_repositories(self, src_repository, src_path, dest_repository, dest_path, delete_source = True):
        """
        Move file from one repository to another repository
        :param src_repository: Source repository
        :param src_path: Source path
        :param dest_repository: Destination repository
        :param dest_path: Destination path
        """
        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')

        try:
            client = self.get_client()
            source_bucket = client.bucket(src_repository)
            source_blob = source_bucket.blob(src_path)
            destination_bucket = client.bucket(dest_repository)

            # Optional: set a generation-match precondition to avoid potential race conditions
            # and data corruptions. The request is aborted if the object's
            # generation number does not match your precondition. For a destination
            # object that does not yet exist, set the if_generation_match precondition to 0.
            # If the destination object already exists in your bucket, set instead a
            # generation-match precondition using its generation number.
            # There is also an `if_source_generation_match` parameter, which is not used in this example.
            destination_generation_match_precondition = 0

            source_bucket.copy_blob(
                source_blob, destination_bucket, dest_path, if_generation_match=destination_generation_match_precondition,
            )
            if delete_source:
                source_bucket.delete_blob(src_path)

            return "Success, file moved"
        except Exception as e:
            raise Exception(f'Error while moving file: {e}')
    
    def move(self, src_path, dest_path):
        """
        Move file from one path to another path in the same repository
        :param src_path: Source path
        :param dest_path: Destination path
        """
        if not self.repository:
            raise Exception('Repository not set')
        
        return self.move_between_repositories(self.repository, src_path, self.repository, dest_path)

    def copy_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        """
        Copy file from one repository to another repository
        :param src_repository: Source repository
        :param src_path: Source path
        :param dest_repository: Destination repository
        :param dest_path: Destination path
        """
        return self.move_between_repositories(src_repository, src_path, dest_repository, dest_path, delete_source=False)
    
    def copy(self, src_path, dest_path):
        """
        Copy file from one path to another path in the same repository
        :param src_path: Source path
        :param dest_path: Destination path
        """
        if not self.repository:
            raise Exception('Repository not set')
        
        return self.copy_between_repositories(self.repository, src_path, self.repository, dest_path)

    def get_metadata(self, file_path):
        """
        Get file metadata
        :param file_path: File path
        """
        if not self.repository:
            raise Exception('Repository not set')

        try:
            client = self.get_client()
            bucket = client.bucket(self.repository)

            # Retrieve a blob, and its metadata, from Google Cloud Storage.
            # Note that `get_blob` differs from `Bucket.blob`, which does not
            # make an HTTP request.
            return bucket.get_blob(file_path)
        except Exception as e:
            raise Exception(f'Error while getting file metadata: {e}')
    
    def sync(self, repository, src_path, dest_path):
        pass

    def get_file_url(self):
        pass

    def sync_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        pass

    def exists(self, repository, file_path):
        pass
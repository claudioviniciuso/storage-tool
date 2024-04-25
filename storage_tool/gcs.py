from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from storage_tool.base import BaseStorage
from storage_tool.data_processor import DataProcessor

def erase_after_pattern(original_string, pattern):
    parts = original_string.split(pattern, 1)
    result = parts[1] if len(parts) > 0 else original_string
    return result

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

    @property
    def client(self):
        try:
            return storage.Client(credentials=self.credentials, project=self.project_id)
        except Exception as e:
            print(e)
            raise Exception(f'Error while getting client: {e}')
        

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

        self.client = Authorization.client

    def list_repositories(self):
        """
        List all repositories
        """
        buckets = self.client.list_buckets()
        list_buckets = []
        for bucket in buckets:
            list_buckets.append({"repository": bucket.name, "created_at": None})

        return list_buckets

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
        try:
            self.client.create_bucket(repository)
        except Exception as e:
            raise Exception(f'Error while creating repository: {e}')

        return "Success, {repository} created".format(repository=repository)

    def set_or_create_repository(self, repository):
        """
        Verify and set repository
        :param repository: Repository name
        """
        repositories = self.list_repositories()
        exists = any(d["repository"] == repository for d in repositories)
        if not exists:
            self.create_repository(repository)
        self.repository = repository

        return "Success, {repository} created and defined".format(repository=repository)


    def read(self, file_path, return_type=pd.DataFrame):
        """
        Read file
        :param file_path: File path
        return: String File content
        """
        if not self.repository:
            raise Exception('Repository not set')

        try:
            bucket = self.client.get_bucket(self.repository)
            blob = bucket.blob(file_path)

            file_extension = file_path.split('.')[-1].lower()
            data = self.process_data(blob.download_as_string(), file_extension, return_type)
            return data

        except Exception as e:
            raise Exception(f'Error while reading file: {e}')


    def put(self, file_path, content):
        """
        Write file to GCS
        :param file_path: File path
        :param content: File content

        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            bucket = self.client.get_bucket(self.repository)
            data = self.convert_to_bytes(content, file_path.split('.')[-1].lower())
            bucket.blob(file_path).upload_from_string(data)
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
        blob_list = self.client.get_bucket(self.repository).list_blobs(prefix=path)
        subfolder_blobs = [blob for blob in blob_list if path in blob.name]
        
        list_files = []
        if not blob_list:
            return list_files
        
        for file in subfolder_blobs:
            if path:
                filename = erase_after_pattern(
                    original_string=file.name,
                    pattern= path
                )
            else:
                filename = file.name
            # Verify if object is a folder or file
            if len(filename.split('/')) > 1:
                list_files.append({"object": f"{filename.split('/')[0]}/", "type": "folder"})
            else:
                list_files.append({"object": filename, "type": "file"})

        # # Return unique items
        list_files = [dict(t) for t in {tuple(d.items()) for d in list_files}]

        return list_files
    
    def delete(self,  file_path):
        """
        Delete file f
        :param file_path: File path

        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            bucket = self.client.bucket(self.repository)
            blob = bucket.blob(file_path)
            blob.delete()
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
            source_bucket = self.client.bucket(src_repository)
            source_blob = source_bucket.blob(src_path)
            destination_bucket = self.client.bucket(dest_repository)

            source_bucket.copy_blob(source_blob, destination_bucket, dest_path)
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
            bucket = self.client.bucket(self.repository)

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
import os, uuid
import pandas as pd

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from storage_tool.base import BaseStorage
from storage_tool.data_processor import DataProcessor


class AzureAuthorization:
    def __init__(self):
        """
        Initialize Azure Authorization
        """
        self.connection_string = None

    def set_credentials(self, connection_string):
        """
        Set credentials to connect and access to Azure

        :param connection_string: AWS Access Key ID
        """
        if not connection_string:
            raise Exception('Azure Connection String is required')

        self.connection_string = connection_string

        return "Success, credentials defined"

    def test_credentials(self):
        """
        Test credentials to connect to S3
        """
        try:
            client = self.client
            containers = client.list_containers()

            for container in containers:
                print(container['name'])

        except Exception as e:
            # print(e)
            return False
        return True

    @property
    def client(self):
        """
        Create BlobServiceClient
        """
        try:
            return BlobServiceClient.from_connection_string(self.connection_string)
        except Exception as e:
            print(e)
            return None


class AzureStorage(BaseStorage, DataProcessor):
    # Define permitted return types
    return_types = [dict, pd.DataFrame, list]

    def __init__(self, Authorization):
        if not isinstance(Authorization, AzureAuthorization):
            raise Exception('Authorization must be an instance of AzureAuthorization class')

        if not Authorization.test_credentials():
            raise Exception('Invalid credentials')

        self.client = Authorization.client
        self.container = None


    def set_container(self, container):
        """
        Verify and set container
        :param container: Container name
        """
        containers = self.list_repositories()
        exists = any(d["repository"] == container for d in containers)
        if not exists:
            raise Exception('Repository not found')

        self.container = container
        return "Success, {container} defined".format(container=container)


    def create_container(self, container):
        """
        Create container
        :param container: container name
        """
        try:
            self.client.create_container(name=container)
        except ResourceExistsError:
            raise Exception('Error while creating container')

        return "Success, {container} created".format(container=container)


    def set_or_create_container(self, container):
        """
        Verify and set container
        :param container: container name
        """
        repositories = self.list_repositories()
        exists = any(d["container"] == container for d in repositories)
        if not exists:
            self.create_container(container)
        self.container = container

        return "Success, {container} created and defined".format(container=container)


    def list_repositories(self):
        """
        List all containers in Azure
        """
        containers = self.client.list_containers(include_metadata=True)
        list_buckets = []

        for container in containers:
            list_buckets.append({"repository": container['name'], "created_at": container['last_modified']})

        return list_buckets


    def list(self, path=''):
        """
        List all files and folders in repository
        :param path: Path to list
        return: List of files and folders
        """
        if not self.container:
            raise Exception('Repository not set')

        container_client = self.client.get_container_client(container= self.container)

        blob_list = container_client.list_blobs()

        list_files = []

        if not blob_list:
            return list_files

        for file in blob_list:
            # Verify if object is a folder or file
            if len(file['name'].split('/')) > 1:
                list_files.append({"object": f"{file['name'].split('/')[0]}/", "type": "folder"})
            else:
                list_files.append({"object": file['name'], "type": "file"})

        # Return unique items
        list_files = [dict(t) for t in {tuple(d.items()) for d in list_files}]

        return list_files


    def read(self, file_path, return_type=pd.DataFrame):
        """
        Read file from Azure
        :param file_path: File path
        :param return_type: Return type (dict, pd.DataFrame, list)
        return: File content
        """
        if not self.container:
            raise Exception('Repository not set')

        try:
            blob_client = self.client.get_blob_client(
                container=self.container,
                blob=file_path
            )
            bytes = blob_client.download_blob().readall()

            file_extension = file_path.split('.')[-1].lower()

            data = self.process_data(bytes, file_extension, return_type)
            return data

        except Exception as e:
            raise Exception(f'Error while reading file: {e}')


    def put(self, file_path, content):
        """
        Write file to Azure
        :param file_path: File path
        :param content: File content

        """
        if not self.container:
            raise Exception('Repository not set')
        try:

            blob_client = self.client.get_blob_client(
                container=self.container,
                blob=file_path
            )

            data = self.convert_to_bytes(content, file_path.split('.')[-1].lower())

            blob_client.upload_blob(data, blob_type="BlockBlob")

            return "Success, file written"
        except Exception as e:
            raise Exception(f'Error while writing file: {e}')


    def delete(self, file_path):
        """
        Delete file from Azure
        :param file_path: File path

        """
        if not self.container:
            raise Exception('Repository not set')
        try:
            blob_client = self.client.get_blob_client(
                container=self.container,
                blob=file_path
            )
            blob_client.delete_blob()

            return "Success, file deleted"

        except Exception as e:
            raise Exception(f'Error while deleting file: {e}')


    def move(self, src_path, dest_path):
        """
        Move file from one path to another path in the same repository
        :param src_path: Source path
        :param dest_path: Destination path
        """
        if not self.repository:
            raise Exception('Repository not set')

        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')
        try:
            response = self.client.copy_object(
                Bucket=self.repository,
                CopySource={'Bucket': self.repository, 'Key': src_path},
                Key=dest_path
            )

            response = self.client.delete_object(
                Bucket=self.repository,
                Key=src_path
            )

            return "Success, file moved"
        except ClientError as e:
            raise Exception(f'Error while moving file: {e}')

    def move_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
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
            response = self.client.copy_object(
                Bucket=dest_repository,
                CopySource={'Bucket': src_repository, 'Key': src_path},
                Key=dest_path
            )
            response = self.client.delete_object(
                Bucket=src_repository,
                Key=src_path
            )

            return "Success, file moved"
        except ClientError as e:
            raise Exception(f'Error while moving file: {e}')


    def copy(self, src_path, dest_path):
        """
        Copy file from one path to another path in the same repository
        :param src_path: Source path
        :param dest_path: Destination path
        """
        if not self.repository:
            raise Exception('Repository not set')

        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')

        try:
            response = self.client.copy_object(
                Bucket=self.repository,
                CopySource={'Bucket': self.repository, 'Key': src_path},
                Key=dest_path
            )

            return "Success, file copied"
        except ClientError as e:
            raise Exception(f'Error while copying file: {e}')
        except Exception as e:
            raise Exception(f'Error while copying file: {e}')

    def copy_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        """
        Copy file from one repository to another repository
        :param src_repository: Source repository
        :param src_path: Source path
        :param dest_repository: Destination repository
        :param dest_path: Destination path
        """
        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')

        try:
            response = self.client.copy_object(
                Bucket=dest_repository,
                CopySource={'Bucket': src_repository, 'Key': src_path},
                Key=dest_path
            )

            return "Success, file copied"
        except ClientError as e:
            raise Exception(f'Error while copying file: {e}')
        except Exception as e:
            raise Exception(f'Error while copying file: {e}')

    def sync(self, src_path, dest_path):
        """
        Sync files from one repository to another repository
        :param src_path: Source path
        :param dest_path: Destination path
        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            response = self.client.list_objects_v2(
                Bucket=self.repository,
                Prefix=src_path
            )
            if response.get('ResponseMetadata').get('HTTPStatusCode') != 200:
                raise Exception('Error while listing files')

            for file in response['Contents']:
                file_path = file['Key']
                file_name = file_path.split('/')[-1]
                dest_file_path = f'{dest_path}/{file_name}'
                self.copy(self.repository, file_path, dest_file_path)

            return "Success, files synced"
        except ClientError as e:
            raise Exception(f'Error while syncing files: {e}')
        except Exception as e:
            raise Exception(f'Error while syncing files: {e}')

    def sync_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        """
        Sync files from one repository to another repository
        :param src_repository: Source repository
        :param src_path: Source path
        :param dest_repository: Destination repository
        :param dest_path: Destination path
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=src_repository,
                Prefix=src_path
            )
            if response.get('ResponseMetadata').get('HTTPStatusCode') != 200:
                raise Exception('Error while listing files')

            for file in response['Contents']:
                file_path = file['Key']
                file_name = file_path.split('/')[-1]
                dest_file_path = f'{dest_path}/{file_name}'
                self.copy_between_repositories(src_repository, file_path, dest_repository, dest_file_path)
            return "Success, files synced"
        except ClientError as e:
            raise Exception(f'Error while syncing files: {e}')
        except Exception as e:
            raise Exception(f'Error while syncing files: {e}')

    def exists(self,  file_path):
        """
        Check if file exists
        :param file_path: File path
        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            response = self.client.head_object(
                Bucket=self.repository,
                Key=file_path
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise Exception(f'Error while checking file existence: {e}')
        except Exception as e:
            raise Exception(f'Error while checking file existence: {e}')

    def get_metadata(self, file_path):
        """
        Get file metadata
        :param file_path: File path
        """
        if not self.repository:
            raise Exception('Repository not set')

        try:
            response = self.client.head_object(
                Bucket=self.repository,
                Key=file_path
            )
            if response.get('ResponseMetadata').get('HTTPStatusCode') != 200:
                raise Exception('Error while getting file metadata')
            return "Success, file metadata"
        except ClientError as e:
            raise Exception(f'Error while getting file metadata: {e}')
        except Exception as e:
            raise Exception(f'Error while getting file metadata: {e}')

    def get_file_url(self, file_path):
        """
        Get file url
        :param file_path: File path

        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            response = self.client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self.repository,
                    'Key': file_path,
                },
                ExpiresIn=120
            )
            if response.get('ResponseMetadata').get('HTTPStatusCode') != 200:
                raise Exception('Error while getting file url')

            return response
        except ClientError as e:
            raise Exception(f'Error while getting file url: {e}')
        except Exception as e:
            raise Exception(f'Error while getting file url: {e}')

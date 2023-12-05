import os, uuid
import pandas as pd

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
from storage_tool.base import BaseStorage
from storage_tool.data_processor import DataProcessor


def erase_after_pattern(original_string, pattern):
    parts = original_string.split(pattern, 1)
    result = parts[1] if len(parts) > 0 else original_string
    return result

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
            client.list_containers()

        except Exception as e:
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
        self.repository = None


    def set_repository(self, repository):
        """
        Verify and set container
        :param container: Container name
        """
        containers = self.list_repositories()
        exists = any(d["repository"] == repository for d in containers)
        if not exists:
            raise Exception('Repository not found')

        self.repository = repository
        return "Success, {container} defined".format(container=repository)


    def create_repository(self, repository):
        """
        Create container
        :param container: container name
        """
        try:
            self.client.create_container(name=repository)
        except ResourceExistsError:
            raise Exception('Error while creating container')

        return "Success, {container} created".format(container=repository)


    def set_or_create_repository(self, repository):
        """
        Verify and set container
        :param container: container name
        """
        repositories = self.list_repositories()
        exists = any(d["container"] == repository for d in repositories)
        if not exists:
            self.create_container(repository)
        self.repository = repository

        return "Success, {container} created and defined".format(container=repository)


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
        if not self.repository:
            raise Exception('Repository not set')

        container_client = self.client.get_container_client(container= self.repository)

        blob_list = container_client.list_blobs()

        # Filter blobs that match the desired subfolder
        subfolder_blobs = [blob for blob in blob_list if path in blob.name]

        # for blob in subfolder_blobs:
        #     print(f"Name: {blob.name}")

        list_files = []

        if not blob_list:
            return list_files

        for file in subfolder_blobs:
            if path:
                filename = erase_after_pattern(
                    original_string=file['name'],
                    pattern= path
                )
            else:
                filename = file['name']
            # Verify if object is a folder or file
            if len(filename.split('/')) > 1:
                list_files.append({"object": f"{filename.split('/')[0]}/", "type": "folder"})
            else:
                list_files.append({"object": filename, "type": "file"})

        # # Return unique items
        list_files = [dict(t) for t in {tuple(d.items()) for d in list_files}]

        return list_files


    def read(self, file_path, return_type=pd.DataFrame):
        """
        Read file from Azure
        :param file_path: File path
        :param return_type: Return type (dict, pd.DataFrame, list)
        return: File content
        """
        if not self.repository:
            raise Exception('Repository not set')

        try:
            blob_client = self.client.get_blob_client(
                container=self.repository,
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
        if not self.repository:
            raise Exception('Repository not set')
        try:

            blob_client = self.client.get_blob_client(
                container=self.repository,
                blob=file_path
            )

            data = self.convert_to_bytes(content, file_path.split('.')[-1].lower())

            blob_client.upload_blob(data, blob_type="BlockBlob")

            return "Success, file written"
        except Exception as e:
            raise Exception(f'Error while writing file: {file_path}')


    def delete(self, file_path):
        """
        Delete file from Azure
        :param file_path: File path

        """
        if not self.repository:
            raise Exception('Repository not set')
        try:
            blob_client = self.client.get_blob_client(
                container=self.repository,
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
            # Create a BlobServiceClient
            src_blob_client = self.client.get_blob_client(
                container=self.repository,
                blob=src_path
            )

            # Get the content of the source blob
            source_blob_content = src_blob_client.download_blob().readall()

            # Create a reference to the destination blob
            destination_blob = self.client.get_blob_client(
                container=self.repository,
                blob=dest_path
            )

            content_setting = ContentSettings(
                content_type=src_blob_client.get_blob_properties().content_settings.content_type
            )
            # Upload the content to the destination blob
            destination_blob.upload_blob(
                source_blob_content,
                content_settings=content_setting
            )

            # Delete the source blob
            src_blob_client.delete_blob()

            return "Success, file moved"

        except Exception as e:
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
            src_blob_client = self.client.get_blob_client(
                container=src_repository,
                blob=src_path
            )

            src_blob_content = src_blob_client.download_blob().readall()

            content_setting = ContentSettings(
                content_type=src_blob_client.get_blob_properties().content_settings.content_type
            )

            dst_blob_client = self.client.get_blob_client(
                container=dest_repository,
                blob=dest_path
            )

            # Upload the content to the destination blob
            dst_blob_client.upload_blob(
                src_blob_content,
                content_settings=content_setting
            )

            src_blob_client.delete_blob()

            return "Success, file moved"

        except Exception as e:
            raise Exception(f'Error while moving file: {e}')


    def copy(self, src_path, dest_path):
        """
        Copy file from one path to another path in the same container
        :param src_path: Source path
        :param dest_path: Destination path
        """
        if not self.repository:
            raise Exception('Container not set')

        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')

        try:
            # Create a BlobServiceClient
            src_blob_client = self.client.get_blob_client(
                container=self.repository,
                blob=src_path
            )

            # Get the content of the source blob
            source_blob_content = src_blob_client.download_blob().readall()

            # Create a reference to the destination blob
            destination_blob = self.client.get_blob_client(
                container=self.repository,
                blob=dest_path
            )

            content_setting = ContentSettings(
                content_type=src_blob_client.get_blob_properties().content_settings.content_type
            )
            # Upload the content to the destination blob
            destination_blob.upload_blob(
                source_blob_content,
                content_settings=content_setting
            )

            return "Success, file copied"

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
            src_blob_client = self.client.get_blob_client(
                container=src_repository,
                blob=src_path
            )

            src_blob_content = src_blob_client.download_blob().readall()

            content_setting = ContentSettings(
                content_type=src_blob_client.get_blob_properties().content_settings.content_type
            )

            dst_blob_client = self.client.get_blob_client(
                container=dest_repository,
                blob=dest_path
            )

            # Upload the content to the destination blob
            dst_blob_client.upload_blob(
                src_blob_content,
                content_settings=content_setting
            )

            return "Success, file copied"

        except Exception as e:
            raise Exception(f'Error while copying file: {e}')


    def sync(self, src_path, dest_path):
        """
        Sync files from one container to another container
        :param src_path: Source path
        :param dest_path: Destination path
        """
        if not self.repository:
            raise Exception('Container not set')

        try:
            container_client = self.client.get_container_client(self.repository)

            # Ensure source path ends with '/'
            src_path = src_path.rstrip('/') + '/'
            dest_path = dest_path.rstrip('/') + '/'

            # List blobs in the source path
            blobs = container_client.walk_blobs(name_starts_with=src_path)

            # Iterate over blobs and copy to the destination path
            for blob in blobs:
                source_blob_name = blob['name']
                dest_blob_name = os.path.join(
                    dest_path,
                    os.path.relpath(source_blob_name, src_path)
                ).replace(os.path.sep, "/")

                # Get blob clients
                source_blob_client = container_client.get_blob_client(source_blob_name)
                destination_blob_client = container_client.get_blob_client(dest_blob_name)

                # Copy blob from source to destination
                destination_blob_client.start_copy_from_url(source_blob_client.url)

            return "Success, files synced"

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

            src_container_client = self.client.get_container_client(container= src_repository)
            dst_container_client = self.client.get_container_client(container= dest_repository)

            # Ensure source path ends with '/'
            src_path = src_path.rstrip('/') + '/'
            dest_path = dest_path.rstrip('/') + '/'

            # List blobs in the source path
            blobs = src_container_client.walk_blobs(name_starts_with=src_path)

            # Iterate over blobs and copy to the destination path
            for blob in blobs:
                source_blob_name = blob['name']
                dest_blob_name = os.path.join(
                    dest_path,
                    os.path.relpath(source_blob_name, src_path)
                ).replace(os.path.sep, "/")

                # Get blob clients
                source_blob_client = src_container_client.get_blob_client(source_blob_name)
                destination_blob_client = dst_container_client.get_blob_client(dest_blob_name)

                # Copy blob from source to destination
                destination_blob_client.start_copy_from_url(source_blob_client.url)

            return "Success, files synced"
        except ClientError as e:
            raise Exception(f'Error while syncing files: {e}')
        except Exception as e:
            raise Exception(f'Error while syncing files: {e}')


    def exists(self, file_path):
        """
        Check if file exists
        :param file_path: File path
        """
        if not self.repository:
            raise Exception('Container not set')

        try:
            blob_client = self.client.get_blob_client(
                container=self.repository,
                blob=file_path
            )

            blob_properties = blob_client.get_blob_properties()

            return True

        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise Exception(f'Error while checking file existence: {e}')


    def get_metadata(self, file_path):
        """
        Get file metadata
        :param file_path: File path
        """
        if not self.repository:
            raise Exception('Container not set')

        try:
            blob_client = self.client.get_blob_client(
                container=self.repository,
                blob=file_path
            )

            blob_properties = blob_client.get_blob_properties()

            return blob_properties

        except ResourceNotFoundError as e:
            return Exception(f'Error while checking file existence: {e}')
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
            blob_client = self.client.get_blob_client(
                container=self.repository,
                blob=file_path
            )

            blob_url = blob_client.url

            return blob_url

        except ResourceNotFoundError as e:
            return Exception(f'Error while checking file existence: {e}')

        except Exception as e:
            raise Exception(f'Error while getting file url: {e}')

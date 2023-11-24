import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from storage_tool.base import BaseStorage
from storage_tool.data_processor import DataProcessor


class S3Authorization:
    def __init__(self):
        self.aws_access_key_id = None
        self.aws_secret_access_key = None
        self.region_name = None

    def set_credentials(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
    
    def test_credentials(self):
        try:
            client = self.client
            client.list_buckets()
        except NoCredentialsError:
            return False
        except ClientError:
            return False
        except Exception as e:
            print(e)
            return False
        return True
    @property
    def client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
    

class S3Storage(BaseStorage, DataProcessor):
    return_types = [dict, pd.DataFrame]
    def __init__(self, S3Authorization):
        self.s3_client = S3Authorization.client
        self.repository = None

    def set_repository(self, repository):
        repositories = self.list_repositories()
        exists = any(d["repository"] == repository for d in repositories)
        if not exists:
            raise Exception('Repository not found')
        self.repository = repository

    def create_repository(self, repository):
        response = self.s3_client.create_bucket(
            Bucket=repository
        )
        return response

    def list_repositories(self):
        response = self.s3_client.list_buckets()
        list_buckets = []

        if response.get('ResponseMetadata').get('HTTPStatusCode') != 200:
            raise Exception('Error while listing repositories')

        for bucket in response['Buckets']:
            list_buckets.append({"repository": bucket['Name'], "created_at": bucket['CreationDate']})

        return list_buckets

    def list(self, path=''):
        if not self.repository:
            raise Exception('Repository not set')

        response = self.s3_client.list_objects_v2(
            Bucket=self.repository,
        )
        list_files = []

        if response.get('ResponseMetadata').get('HTTPStatusCode') != 200:
            raise Exception('Error while listing files')

        for file in response['Contents']:
            # If key is first level append to list
            if len(file['Key'].split('/')) >1:
                list_files.append({"object": f"{file['Key'].split('/')[0]}/", "type": "folder"})
            else:  
                list_files.append({"object": file['Key'], "type": "file"})
        # Return unique items
        list_files = [dict(t) for t in {tuple(d.items()) for d in list_files}]
    
        return list_files

    def read(self, file_path, return_type=pd.DataFrame):
        if not self.repository:
            raise Exception('Repository not set')
        try:
            response = self.s3_client.get_object(
                Bucket=self.repository,
                Key=file_path
            )
            file_extension = file_path.split('.')[-1].lower()
            data = self.process_data(response['Body'].read(), file_extension, return_type)
            return data

        except ClientError as e:
            raise Exception(f'Error while reading file: {e}')
        except Exception as e:
            raise Exception(f'Error while reading file: {e}')
    
    def put(self, file_path, content):
        if not self.repository:
            raise Exception('Repository not set')
        try:
            data = self.convert_to_bytes(content, file_path.split('.')[-1].lower())
            response = self.s3_client.put_object(
                Bucket=self.repository,
                Key=file_path,
                Body=data
            )
            return response
        except ClientError as e:
            raise Exception(f'Error while writing file: {e}')
        except Exception as e:
            raise Exception(f'Error while writing file: {e}')
    
    def delete(self,  file_path):
        if not self.repository:
            raise Exception('Repository not set')
        try:
            response = self.s3_client.delete_object(
                Bucket=self.repository,
                Key=file_path
            )
            print(response)
            return response
        except ClientError as e:
            raise Exception(f'Error while deleting file: {e}')
        except Exception as e:
            raise Exception(f'Error while deleting file: {e}')
    
    def move(self, src_path, dest_path):
        if not self.repository:
            raise Exception('Repository not set')
        
        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')
        try:
            response = self.s3_client.copy_object(
                Bucket=self.repository,
                CopySource={'Bucket': self.repository, 'Key': src_path},
                Key=dest_path
            )
            response = self.s3_client.delete_object(
                Bucket=self.repository,
                Key=src_path
            )
            return response
        except ClientError as e:
            raise Exception(f'Error while moving file: {e}')
        except Exception as e:
            raise Exception(f'Error while moving file: {e}')
        
    def move_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')

        try:
            response = self.s3_client.copy_object(
                Bucket=dest_repository,
                CopySource={'Bucket': src_repository, 'Key': src_path},
                Key=dest_path
            )
            response = self.s3_client.delete_object(
                Bucket=src_repository,
                Key=src_path
            )
            return response
        except ClientError as e:
            raise Exception(f'Error while moving file: {e}')
        except Exception as e:
            raise Exception(f'Error while moving file: {e}')
        
    def copy(self, src_path, dest_path):
        if not self.repository:
            raise Exception('Repository not set')
        
        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')

        try:
            response = self.s3_client.copy_object(
                Bucket=self.repository,
                CopySource={'Bucket': self.repository, 'Key': src_path},
                Key=dest_path
            )
            return response
        except ClientError as e:
            raise Exception(f'Error while copying file: {e}')
        except Exception as e:
            raise Exception(f'Error while copying file: {e}')
    
    def copy_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        if src_path.split('.')[-1].lower() != dest_path.split('.')[-1].lower():
            raise Exception('File extension must be the same')

        try:
            response = self.s3_client.copy_object(
                Bucket=dest_repository,
                CopySource={'Bucket': src_repository, 'Key': src_path},
                Key=dest_path
            )
            return response
        except ClientError as e:
            raise Exception(f'Error while copying file: {e}')
        except Exception as e:
            raise Exception(f'Error while copying file: {e}')
        
    def sync(self, src_path, dest_path):
        if not self.repository:
            raise Exception('Repository not set')
        """
        Sync files from one repository to another repository
        """
        try:
            response = self.s3_client.list_objects_v2(
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
            return response
        except ClientError as e:
            raise Exception(f'Error while syncing files: {e}')
        except Exception as e:
            raise Exception(f'Error while syncing files: {e}')
    
    def sync_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        """
        Sync files from one repository to another repository
        """
        try:
            response = self.s3_client.list_objects_v2(
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
            return response
        except ClientError as e:
            raise Exception(f'Error while syncing files: {e}')
        except Exception as e:
            raise Exception(f'Error while syncing files: {e}')

    def exists(self,  file_path):
        if not self.repository:
            raise Exception('Repository not set')
        """
        Check if file exists in S3 bucket
        """
        try:
            response = self.s3_client.head_object(
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
        if not self.repository:
            raise Exception('Repository not set')
        """
        Get metadata from file
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.repository,
                Key=file_path
            )
            return response
        except ClientError as e:
            raise Exception(f'Error while getting file metadata: {e}')
        except Exception as e:
            raise Exception(f'Error while getting file metadata: {e}')
        
    def get_file_url(self, file_path):
        if not self.repository:
            raise Exception('Repository not set')
        """
        Get file url
        """
        try:
            response = self.s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self.repository,
                    'Key': file_path,
                },
                ExpiresIn=120
            )
            return response
        except ClientError as e:
            raise Exception(f'Error while getting file url: {e}')
        except Exception as e:
            raise Exception(f'Error while getting file url: {e}')
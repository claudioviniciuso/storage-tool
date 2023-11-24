import pandas as pd
import os
import json
from storage_tool.base import BaseStorage
from storage_tool.data_processor import DataProcessor


class LocalStorage(BaseStorage, DataProcessor):
    def __init__(self) -> None:
        self.repository = None
    
    def set_repository(self, repository):
        """
        Verify if repository exists and set repository
        """
        if not os.path.isdir(repository):
            raise ValueError('Repository does not exist')
        self.repository = repository
            
    def create_repository(self, repository):
        """
        Create repository
        """
        if os.path.isdir(repository):
            raise ValueError('Repository already exists')
        os.mkdir(repository)

    def list_repositories(self):
        """
        List repositories 
        """
        list_ = []
        response = os.listdir()
        for item in response:
            if os.path.isdir(item):
                #Verify item is different from file
                list_.append({"repository": item, "created_at": None})

        return list_
    
    def list(self, path=''):
        """
        List files in path
        """
        response = os.listdir(os.path.join(self.repository, path))
        list_ = []
        for item in response:
            if os.path.isfile(os.path.join(self.repository, path, item)):
                list_.append({"object": item, "type": "file"})
            elif os.path.isdir(os.path.join(self.repository, path, item)):
                list_.append({"object": "f{item}/", "type": "folder"})
        
        return list_
    
    def read(self, file_path, return_type=None):
        """
        Read file
        """
        file_extension = file_path.split('.')[-1]
        with open(os.path.join(self.repository, file_path), 'rb') as f:
            data_bytes = f.read()
        return self.process_data(data_bytes, file_extension, return_type)
    
    def put(self, file_path, content):
        """
        Put file
        """
        file_extension = file_path.split('.')[-1]
        data_bytes = self.convert_to_bytes(content, file_extension)

        if not os.path.isdir(os.path.join(self.repository, os.path.dirname(file_path))):
            os.makedirs(os.path.join(self.repository, os.path.dirname(file_path)))

        with open(os.path.join(self.repository, file_path), 'wb') as f:
            f.write(data_bytes)
    
    def delete(self, file_path):
        """
        Delete file
        """
        os.remove(os.path.join(self.repository, file_path))

    def move(self, src_path, dest_path):
        """
        Move file
        """
        if not os.path.isdir(os.path.join(self.repository, os.path.dirname(dest_path))):
            os.makedirs(os.path.join(self.repository, os.path.dirname(dest_path)))

        os.rename(os.path.join(self.repository, src_path), os.path.join(self.repository, dest_path))

    def move_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        """
        Move file between repositories
        """
            
        os.makedirs(os.path.join(dest_repository, os.path.dirname(dest_path)), exist_ok=True)

        self.copy_between_repositories(src_repository, src_path, dest_repository, dest_path)
        
        self.delete(src_path)
    
    def copy(self, src_path, dest_path):
        """
        Copy file
        """
        os.makedirs(os.path.join(self.repository, os.path.dirname(dest_path)), exist_ok=True)
        with open(os.path.join(self.repository, src_path), 'rb') as f:
            data_bytes = f.read()
        with open(os.path.join(self.repository, dest_path), 'wb') as f:
            f.write(data_bytes)

    def copy_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        """
        Copy file between repositories
        """
        os.makedirs(os.path.join(dest_repository, os.path.dirname(dest_path)), exist_ok=True)
        with open(os.path.join(src_repository, src_path), 'rb') as f:
            data_bytes = f.read()
        with open(os.path.join(dest_repository, dest_path), 'wb') as f:
            f.write(data_bytes)
    
    def sync(self, src_path, dest_path):
        """
        Sync files from one folder to another
        """
        raise NotImplementedError
    
    def sync_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        """
        Sync file between repositories
        """
        raise NotImplementedError
    
    def exists(self, file_path):
        """
        Verify if file exists
        """
        return os.path.isfile(os.path.join(self.repository, file_path))

    def get_metadata(self, file_path):
        """
        Get metadata from file
        """
        return os.stat(os.path.join(self.repository, file_path))
    
    def get_file_url(self, file_path):
        """
        Get file url
        """
        return os.path.join(self.repository, file_path)
    
        
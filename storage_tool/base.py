from abc import ABC, abstractmethod

class BaseStorage(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def list_repositories(self):
        pass

    @abstractmethod
    def list(self, repository, path):
        pass
    
    @abstractmethod
    def read(self, file_path, return_type=None):
        pass

    @abstractmethod
    def put(self, repository, file_path, content):
        pass

    @abstractmethod
    def delete(self, repository, file_path):
        pass

    @abstractmethod
    def move(self, repository, src_path, dest_path):
        pass

    @abstractmethod
    def move_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        pass

    @abstractmethod
    def copy(self, repository, src_path, dest_path):
        pass

    @abstractmethod
    def copy(self, src_repository, src_path, dest_repository, dest_path):
        pass

    @abstractmethod        
    def sync(self, repository, src_path, dest_path):
        pass

    @abstractmethod        
    def sync_between_repositories(self, src_repository, src_path, dest_repository, dest_path):
        pass

    @abstractmethod
    def exists(self, repository, file_path):
        pass

    @abstractmethod
    def get_metadata(self, repository, file_path):
        pass

    @abstractmethod
    def get_file_url(self, repository, file_path):
        pass
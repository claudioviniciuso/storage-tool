from storage_tool.azure import AzureAuthorization, AzureStorage
from storage_tool.s3 import S3Storage, S3Authorization
from storage_tool.gcs import GCSStorage, GCSAuthorization
from storage_tool.local import LocalStorage

class Auth:
    def __init__(self, storage_type) -> None:
        self.storage_type = storage_type.upper()
        self.authenticator = None
        self.set_auth()
        pass

    def set_auth(self):
        if self.storage_type == 'S3':
            self.authenticator = S3Authorization()
        elif self.storage_type == 'LOCAL':
            self.authenticator = None
        elif self.storage_type == 'GCS':
            self.authenticator = GCSAuthorization()
        elif self.storage_type == 'AZURE':
            self.authenticator = AzureAuthorization()
        else:
            raise NotImplementedError

class Storage:
    def __init__(self, storage_type, authorization) -> None:
        self.storage_type = storage_type.upper()
        self.authorization = authorization
        self.storage = None

    def get_model(self):
        if self.storage_type == 'S3':
            self.storage = S3Storage(self.authorization)

        elif self.storage_type == 'LOCAL':
            self.storage = LocalStorage()

        elif self.storage_type == 'GCS':
            self.storage = GCSStorage()

        elif self.storage_type == 'AZURE':
            self.storage = AzureStorage(self.authorization)
        else:
            raise NotImplementedError
        return self.storage

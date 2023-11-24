import pandas as pd
import json
from storage_tool.s3 import S3Storage, S3Authorization

from storage_tool import Storage, Auth

with open('secrets/secret_aws.json') as json_file:
    data = json.load(json_file)
    KEY = data['KEY']
    SECRET = data['SECRET']
    REGION = data['REGION']


auth = Auth('S3')
auth = auth.authenticator
auth.set_credentials(KEY, SECRET, REGION)

storage = Storage('S3', auth)
storage = storage.get_model()

storage.set_repository('fortify-raw')

#authorization = S3Authorization()
#authorization.set_credentials(KEY, SECRET, REGION)
#storage = S3Storage(authorization)


#List Repositories
#repositories = storage.list_repositories()
#print(repositories)

#storage.set_repository('fortify-raw')

#storage.create_repository('lib-b')

#storage.set_repository('fortify-raw')
#print(storage.list('folder_A'))

#data = storage.read(repository='import-data-glue', file_path='rdstation/br-fgv-suprema/2022-11-15/deals/data.json', return_type=dict)
#print(type(data))
#storage.put(repository='fortify-raw', file_path='teste.csv', content=data)
#storage.put(repository='fortify-raw', file_path='teste.parquet', content=data)


#Create Fake File 1
#data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

#storage.set_repository('lib-a')
#data = storage.put(file_path='folder_A/teste_A.csv', content=data_fake)
#data = storage.put(file_path='folder_A/teste_B.csv', content=data_fake)
#data = storage.put(file_path='folder_A/teste_C.csv', content=data_fake)

#storage.set_repository('lib-b')
#data = storage.put(file_path='folder_A/teste_A.csv', content=data_fake)
#data = storage.put(file_path='folder_A/teste_B.csv', content=data_fake)


#storage.copy_between_repositories(src_repository='lib-a', src_path='folder_A/teste_A.csv', dest_repository='lib-b', dest_path='folder_A/teste_A_copy.csv')
#storage.move_between_repositories(src_repository='lib-a', src_path='folder_A/teste_A.csv', dest_repository='lib-b', dest_path='folder_A/teste_A_move.csv')

#storage.sync_between_repositories(src_repository='lib-a', src_path='folder_A', dest_repository='lib-b', dest_path='folder_A')


#data = storage.put(repository='fortify-raw', file_path='folder_A/teste_B.json', content=data_fake)
#data = storage.put(repository='fortify-raw', file_path='folder_A/teste_C.parquet', content=data_fake)

#data = storage.put(repository='fortify-raw', file_path='folder_B/teste_A.csv', content=data_fake)


#print(storage.get_file_url(repository='fortify-raw', file_path='folder_A/teste_A.csv'))


#storage.copy(repository='fortify-raw', src_path='teste_copy.parquet', dest_path='teste.parquet')
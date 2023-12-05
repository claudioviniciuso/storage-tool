## Importação da Lib
import os
import json
import pandas as pd

from datetime import datetime
from dotenv import load_dotenv

from storage_tool import Storage, Auth

# Definir Storage Type
STORAGE_TYPE = 'Azure'
load_dotenv('.env.testing')
azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# Autenticação
# Cria um objeto de autenticação e define as credenciais
auth = Auth(STORAGE_TYPE).authenticator
auth.set_credentials(azure_storage_connection_string)

# auth.test_credentials()

# Cria um objeto de Storage
storage = Storage(STORAGE_TYPE, auth).get_model()

# Listar Containers
# print("Lista de containers: ", storage.list_repositories())

# Criar Repositório
try:
    storage.create_container("localtest")
except Exception:
    print('already created!')
    pass


# # Selecionar Repositório
try:
    storage.set_container(container='localtest')
except Exception:
    print('correct! this not exist!')
    pass

# storage.set_container(container='lab-xpto')

# # Listar Objetos
print("Lista de arquivos:", storage.list())


# file01
# file02
# folderA
# ├─ file03
# ├─ file04
# └─ subfolderA
#         ├─ file05
#         └─ file06

data_fake = [{'col1': 1, 'col2': 2}]
countfile = 1
files = []

for idx in range(2):
    filename = f'file{countfile}.csv'
    files.append(filename)
    try:
        storage.put(file_path=f'{filename}', content=data_fake)
    except:
        pass
    countfile += 1

path = 'folderA/'

for idx in range(2):
    filename = f'{path}file{countfile}.csv'
    files.append(filename)
    try:
        storage.put(file_path=filename, content=data_fake)
    except:
        pass
    countfile += 1

subpath = 'folderA/subfolderA/'

for idx in range(2):
    filename = f'{subpath}file{countfile}.csv'
    files.append(filename)
    try:
        storage.put(file_path=filename, content=data_fake)
    except:
        pass
    countfile += 1


print('======= folderA/')
print(storage.list(path='folderA/'))

# folderA
# ├─ file03
# ├─ file04
# └─ subfolder

print('======= subfolderA/')
print(storage.list(path='subfolderA/'))

# └─ subfolderA
#         ├─ file05
#         └─ file06

print('======= folderB/')
print(storage.list(path='folderB/'))

# # # PUT Object
# data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]
# # try:
# #     storage.put(file_path='folder_a/teste_A.csv', content=data_fake)
# #     storage.put(file_path='folder_a/teste_B.json', content=data_fake)
# #     storage.put(file_path='folder_a/teste_C.parquet', content=data_fake)
# # except:
# #     pass

# # # # Read to Dict
# # # data = storage.read(file_path='teste_B.json', return_type=dict)
# # # print("Dados do arquivo teste_B.json: ", data)

# # # # Read to DataFrame
# # # data = storage.read(file_path='teste_B.json', return_type=pd.DataFrame)
# # # print("Dados do arquivo teste_B.json: ")
# # # print(data)

# # # # Delete
# # # storage.delete(file_path='teste_B.json')

# # # # Move
# # try:
# #     storage.move(src_path='teste_A.csv', dest_path='folder_teste/teste_A.csv')
# # except:
# #     print('already moved the file')
# #     pass
# # # Move Between Repositories
# # try:
# #     storage.move_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_A.csv', dest_repository='testing', dest_path='folder_teste/teste_A_move.csv')
# # except:
# #     print('already moved the file')
# #     pass

# # # # Copy
# # try:
# #     storage.copy(src_path='folder_teste/teste_D.csv', dest_path='folder_teste/teste_D_copy.csv')
# # except:
# #     print('already moved the file')
# #     pass

# # # # Copy Between Repositories
# # try:
# #     storage.copy_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_D.csv', dest_repository='exemple-lib', dest_path='folder_teste_x/teste_D_copy.csv')
# # except:
# #     print('already moved the file')
# #     pass
# # # Exists
# print("Arquivo existe (teste_A.csv)? ",
#     storage.exists(file_path='teste_A.csv'))

# print("Arquivo existe (folder_teste/teste_D.csv)? ",
#     storage.exists(file_path='folder_teste/non_existing_file.csv'))

# # # Get Metadata
# # print("Metadados do arquivo: ", storage.get_metadata(file_path='teste_A.csv'))

# # # Get File URL
# print("URL do arquivo: ", storage.get_file_url(file_path='teste_A.csv'))

# # Testing sync between paths from same container
# try:
#     storage.put(file_path='folder_a/file001.csv', content=data_fake)
#     storage.put(file_path='folder_a/file002.json', content=data_fake)
#     storage.put(file_path='folder_a/file003.parquet', content=data_fake)
# except:
#     print('arquivos já criados!')
#     pass

# storage.sync('folder_a', 'folder_b')

# storage.sync_between_repositories(
#     'lab-xpto', 'folder_a',
#     'testing', 'folder_b'
# )

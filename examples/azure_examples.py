## Importação da Lib
import json
import pandas as pd
from storage_tool import Storage, Auth

# Definir Storage Type
STORAGE_TYPE = 'Azure'

# Autenticação
# Cria um objeto de autenticação e define as credenciais
auth = Auth(STORAGE_TYPE).authenticator
auth.set_credentials('AZURE_CONNECTION_STRING')

# auth.test_credentials()

# Cria um objeto de Storage
storage = Storage(STORAGE_TYPE, auth).get_model()

# Listar Containers
print("Lista de containers: ", storage.list_repositories())

# Criar Repositório
try:
    storage.create_container("lab-xpto")
except Exception:
    print('already created!')
    pass


# Selecionar Repositório
try:
    storage.set_container(container='non-exist')
except Exception:
    print('correct! this not exist!')
    pass

storage.set_container(container='lab-xpto')

# Listar Objetos
print("Lista de arquivos:", storage.list())

# PUT Object
data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]
storage.put(file_path='teste_A.csv', content=data_fake)
# storage.put(file_path='teste_B.json', content=data_fake)
# storage.put(file_path='teste_C.parquet', content=data_fake)

# # Read to Dict
# data = storage.read(file_path='teste_B.json', return_type=dict)
# print("Dados do arquivo teste_B.json: ", data)

# # Read to DataFrame
# data = storage.read(file_path='teste_B.json', return_type=pd.DataFrame)
# print("Dados do arquivo teste_B.json: ")
# print(data)

# # Delete
# storage.delete(file_path='teste_B.json')

# # Move
# storage.move(src_path='teste_A.csv', dest_path='folder_teste/teste_A.csv')

# # Move Between Repositories
# storage.move_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_A.csv', dest_repository='exemple-lib', dest_path='folder_teste/teste_A_move.csv')

# # Copy
# storage.copy(src_path='folder_teste/teste_D.csv', dest_path='folder_teste/teste_D_copy.csv')

# # Copy Between Repositories
# storage.copy_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_D.csv', dest_repository='exemple-lib', dest_path='folder_teste_x/teste_D_copy.csv')

# # Exists
# print("Arquivo existe? ", storage.exists(file_path='folder_teste/teste_D.csv'))

# # Get Metadata
# print("Metadados do arquivo: ", storage.get_metadata(file_path='folder_teste/teste_D.csv'))

# # Get File URL
# print("URL do arquivo: ", storage.get_file_url(file_path='folder_teste/teste_D.csv'))

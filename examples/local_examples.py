## Importação da Lib
import json
import pandas as pd
from storage_tool import Storage, Auth


# Definir Storage Type
STORAGE_TYPE = 'LOCAL'

# Autenticação
# Cria um objeto de autenticação e define as credenciais
auth = Auth(STORAGE_TYPE).authenticator

# Cria um objeto de Storage
storage = Storage(STORAGE_TYPE, auth).get_model()

# Listar Repositórios
print("Lista de repositórios: ", storage.list_repositories())

# Criar Repositório
#storage.create_repository("lab-xpto")

# Selecionar Repositório
storage.set_repository(repository='lab-xpto')

# Listar Objetos
print("Lista de arquivos:", storage.list())

# PUT Object
data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]
#print("Create file teste A:", storage.put(file_path='teste_A.csv', content=data_fake))
#print("Create file teste B:", storage.put(file_path='teste_B.json', content=data_fake))
#print("Create file teste C:", storage.put(file_path='teste_C.parquet', content=data_fake))

# Read to Dict
data = storage.read(file_path='teste_B.json', return_type=dict)
print("Dados do arquivo teste_B.json: ", data)

# Read to DataFrame
data = storage.read(file_path='teste_B.json', return_type=pd.DataFrame)
print("Dados do arquivo teste_B.json: ")
print(data)

# Delete
print("Delete file B:", storage.delete(file_path='teste_B.json'))

# Move
print("Move teste A:", storage.move(src_path='teste_A.csv', dest_path='folder_teste/teste_A.csv'))

# Move Between Repositories
print("Move lab-xpto to exemple-lib", storage.move_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_A.csv', dest_repository='exemple-lib', dest_path='folder_teste/teste_A_move.csv'))

# Copy
print("Copy:", storage.copy(src_path='folder_teste/teste_D.csv', dest_path='folder_teste/teste_D_copy.csv'))

# Copy Between Repositories
print("Copy between repositories" ,storage.copy_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_D.csv', dest_repository='exemple-lib', dest_path='folder_teste_x/teste_D_copy.csv'))

# Exists
#print("Arquivo existe? ", storage.exists(file_path='folder_teste/teste_D.csv'))

# Get Metadata
#print("Metadados do arquivo: ", storage.get_metadata(file_path='folder_teste/teste_D.csv'))

# Get File URL
#print("URL do arquivo: ", storage.get_file_url(file_path='folder_teste/teste_D.csv'))
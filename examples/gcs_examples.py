## Importação da Lib
import json
from storage_tool import Storage, Auth

# Definir Storage Type
STORAGE_TYPE = 'GCS'


# Autenticação
repository = "my-repository"
project_id = "project_id"
client_id = "client_id"
client_email = "client_email"
private_key = "private_key"
private_key_id = "private_key_id"

# Cria um objeto de autenticação e define as credenciais
auth = Auth(STORAGE_TYPE).authenticator
auth.set_credentials(project_id, client_id, client_email, private_key, private_key_id)

# Cria um objeto de Storage
storage = Storage(STORAGE_TYPE, auth).get_model()

# Listar Repositórios
print("Lista de repositórios: ", storage.list_repositories())

# Criar Repositório
#print(storage.create_repository("lab-xpto"))

# Selecionar Repositório
print(storage.set_repository(repository=repository))

# Listar Objetos
print("Lista de arquivos:", storage.list())

# PUT Object
data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]
storage.put(file_path='teste_A.csv', content=json.dumps(data_fake))
storage.put(file_path='teste_B.json', content=json.dumps(data_fake))
storage.put(file_path='teste_C.parquet', content=json.dumps(data_fake))

# Read 
data = storage.read(file_path='teste_B.json')
print("Dados do arquivo teste_B.json: ", data)

# Delete
storage.delete(file_path='teste_B.json')

# Move
storage.move(src_path='teste_A.csv', dest_path='folder_teste/teste_A.csv')

# Move Between Repositories
storage.move_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_A.csv', dest_repository='exemple-lib', dest_path='folder_teste/teste_A_move.csv')

# Copy
storage.put(file_path='folder_teste/teste_D.csv', content=json.dumps(data_fake))
storage.copy(src_path='folder_teste/teste_D.csv', dest_path='folder_teste/teste_D_copy.csv')

# Copy Between Repositories
storage.copy_between_repositories(src_repository='lab-xpto', src_path='folder_teste/teste_D.csv', dest_repository='exemple-lib', dest_path='folder_teste_x/teste_D_copy.csv')

# Get Metadata
print("Metadados do arquivo: ", storage.get_metadata(file_path='folder_teste/teste_D.csv'))
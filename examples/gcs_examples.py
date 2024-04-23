## Importação da Lib
import json
from storage_tool import Storage, Auth

# Definir Storage Type
STORAGE_TYPE = 'GCS'


# Autenticação
repository = "lab-xpto"
project_id = "labgooru"
client_id = "116854079973672440157"
client_email = "airflow-bucket@labgooru.iam.gserviceaccount.com"
private_key = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDFMAVKcyBJsg7J\nPLyf8rpsjW1cymzpj4+TDpB0/CAj3Lhc8A9mf6BvfM7verczAkr0ZsQBKHxDHN7s\n9qWZguQuKHgQI6OsRzmwYk5myCHm0BVO6bIvfcbbrnSy/yKkjZwY/A8LTNtC1m0N\nf/cm8wKT5PFI095yjiGEapcWt1zyIlvrwDyhNnXwV1CXY/QgjTxwEUdQLALJHpjC\nRvApI5xlgFbsocDQ8Niek26zhmeIvBc/b2KKG8Zu/AWdKWLz6xKd6DOHTAx0KWyh\n7Rxey+d85Jr8ArC+iMUFUwhEr0sHiu1a7fwbu0ZxlxSNxV8rilC5THTHlMlenOS5\nIQhYMk5LAgMBAAECggEAXHlROeF0xmrdGU3FDePIvqiynX4HBp9/TxFY+KvWrMrR\noTApoMGArVgkOdXcfWz5VtWs3PguElFDUHC5J51lQipTaAo2s0/xw4DO0M/Rakuy\neoAJQtFjBVoyu1EAX5hTmLn4mi2QdC9YFCkrcJYtYstOxn8pxqJWIKHWOl6ClcBo\nuJKgkd1C2UmW8HvifH/sbOBCY/SxQGq2OsxnD3SWbfNw0f+xPlf3hwd+4PLZE0qa\n44w7H+YFYA/1+7b6TlW8g4S2YXaAgDxXTldA2QFlM9sfGZgfqN3AZcUplULajfaF\nE9sk0/BsJxC00NJqNCE2rkUNCK5F6fLfvA8pSIELvQKBgQD1eyGH9nG3Vp+XPAz+\nnF/PpZs9N3+6YFXcIYNBE2fSbpS7cibgioJ4wPbS8r4WKdjltjbR0D3oscJORJsM\nVxfF5DRpQjmRTm+2DM6ixEDCiRXnjHG1yEac2gFa3vRq7+zO+K3CzNMa5BDuu/Ea\n/fS1lVeUfSNcuqo3HkMvHCXJVQKBgQDNox9TwSndqQ+Z/NcFCgPYYAW099VoVLGk\ncHcaxdL+32Dcu0H0gLXRG6t141fsxFiusCwi7UIDL/1D4u7EhDYu92vZk+pFaqTs\nNKbOD8lCVh7uGiaOvxbhGkKLRb/w8KO8jUU4T+q2yHW6MOhJK9G6rgw6sBev0LDs\noDGxkp85HwKBgQCHtrMq/8yOl41ThxeIC9vMaLoSdgRffbL6KGzwJVJYvFnt54Ym\nzWykZcoPhbQhfLE1Di/wfzg61UufCb7Oa7fw5+Ex8DLzanHIK/xxcB59blx1zudu\noHKpdL0bB/gIxuwc4M7vy11KmJvj4HPDgHMxkIcCyMwsD+ba4hgyi1U4oQKBgQC3\nMjiJdA+pIqD8jWy9V4O/cyQCabwcWz97AJqLJmvnlfeEDesfOL9BkEX7G1NMYkuj\nLN3VK8tgbZNStEwElMh0pQXW61iNCQnuSKXF8/hXecPKWU+6YfCvD6byzmvF6Yvl\nQXHGTiQLPKDtA/8cmMYaak4IxrIDdob392ruCHKC/wKBgG5CjXANHcFpGBh9ALnt\nruEA3bfSzVJ9rGxHOc7WrTWcgw4n24fAFmZB4iIZGjLJGdKJljFE/oTTNYQPZo7q\npshxbq6vYih+ju7n7HJjEX+qN0RcT1TgufuGo+J7PmS52vwRUY9LS1JtWpbjt3TB\nnNenNpH2dJRgsJIoQhs5bxY2\n-----END PRIVATE KEY-----\n"
private_key_id = "6ce7192961cd923fd590264ff53f7ff85716e213"

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
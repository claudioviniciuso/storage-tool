# Azure

Este documento descreve a implementação que fornece funcionalidades para interagir com o Azure Blob Storage. A biblioteca oferece métodos para realizar operações comuns, como leitura, escrita, exclusão, movimentação e sincronização de arquivos no Azure Blob Storage.

## Pré-requisitos

Antes de usar esta biblioteca, é necessário configurar as credenciais de autenticação do Azure. As credenciais são fornecidas por meio de uma string de conexão do Azure Storage. Certifique-se de ter as permissões adequadas para acessar os recursos desejados no Azure Blob Storage. Qualquer para obtenção das credenciais, [acesse a documentação](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-cli%2Csign-in-azure-cli).

## Instalação

A biblioteca depende de várias bibliotecas externas, que estão listadas no `requirements.txt`.


## Utilização

### Autenticação no Azure

Antes de usar as funcionalidades da biblioteca, é necessário autenticar-se no Azure Blob Storage. A classe AzureAuthorization é responsável por lidar com a autenticação e testar a validade das credenciais.

Exemplo de uso:

```python

from storage_tool import Storage, Auth
# Inicializa a autorização do Azure
STORAGE_TYPE = 'Azure'
auth = Auth(STORAGE_TYPE).authenticator

# Define as credenciais de conexão
auth.set_credentials("sua_string_de_conexao_do_azure")

# Testa a validade das credenciais
if auth.test_credentials():
    print("Credenciais válidas.")
else:
    print("Credenciais inválidas.")
```

### Manipulação de Armazenamento Azure

A classe `AzureStorage` a principal interface para interagir com o Azure Blob Storage. Ela herda funcionalidades de `BaseStorage` e `DataProcessor`, fornecendo métodos para operações como leitura, escrita, exclusão, movimentação e sincronização de arquivos.

Exemplo de uso:

```python
from storage_tool import Storage, Auth

# Definir Storage Type
STORAGE_TYPE = 'Azure'

# Autenticação
# Cria um objeto de autenticação e define as credenciais
auth = Auth(STORAGE_TYPE).authenticator
auth.set_credentials("sua_string_de_conexao_do_azure")

# Cria um objeto de Storage
storage = Storage(STORAGE_TYPE, auth).get_model()

# Lista todos os repositórios (contêineres) no Azure Blob Storage
repositories = storage.list_repositories()
print(repositories)

# Lista todos os arquivos no contêiner atual
files = storage.list()
print(files)

# Lê o conteúdo de um arquivo no Azure Blob Storage
file_content = storage.read("caminho/do/seu/arquivo.csv")
print(file_content)

```

### Operações de Manipulação de Arquivos

A classe AzureStorage fornece métodos para realizar diversas operações com arquivos, como `put`, `get`, `delete`, `move`, `copy`, `sync` e outras.

Exemplo de uso:

```python
# Escreve um arquivo no Azure Blob Storage
storage.put("novo_arquivo.csv", {"coluna1": [1, 2, 3], "coluna2": ["a", "b", "c"]})

# Move um arquivo dentro do mesmo contêiner
storage.move("arquivo_antigo.csv", "novo_caminho/arquivo_antigo.csv")

# Copia um arquivo dentro do mesmo contêiner
storage.copy("arquivo_original.csv", "copia_do_arquivo.csv")

# Sincroniza arquivos de um caminho para outro no mesmo contêiner
storage.sync("caminho_origem/", "caminho_destino/")

```

Esta biblioteca fornece uma interface simplificada para interagir com o Azure Blob Storage. Certifique-se de configurar corretamente as credenciais e consulte a documentação para obter informações detalhadas sobre os métodos disponíveis.

> Nota: Este é um exemplo simplificado. Considere ajustar o código para atender aos requisitos específicos do seu projeto.

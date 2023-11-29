

Este documento estabelece o escopo e os contornos do projeto `storage-tool`, uma biblioteca Python planejada para fornecer uma interface de programação abstrata e unificada para a gestão de operações de armazenamento de dados em uma variedade de ambientes, incluindo provedores de nuvem populares e sistemas de arquivos locais. A intenção é criar uma camada de abstração que orquestra interações com AWS S3, Google Cloud Storage, Azure Blob Storage, e outros, permitindo aos usuários executar operações como leitura, escrita, listagem, exclusão e transferência de arquivos de forma transparente e eficiente. Este escopo documental descreverá as funcionalidades propostas, os requisitos de sistema, as dependências necessárias, e a arquitetura preliminar da biblioteca. Servirá como uma referência central para as etapas subsequentes de desenvolvimento, garantindo que todos os aspectos do projeto sejam meticulosamente planejados e alinhados com as necessidades dos usuários finais e os objetivos da biblioteca.

  

  

# Definição

**Nome da Biblioteca: storage-tool**

  

## Propósito

A **`storage-tool`** é uma biblioteca Python projetada para abstrair e orquestrar operações de armazenamento de dados em várias plataformas de nuvem e armazenamentos locais. Ela serve como um provedor para o Apache Airflow, permitindo que tarefas de fluxo de trabalho de dados interajam com sistemas de armazenamento de forma unificada e agnóstica à plataforma.

## Funcionalidades chave

*   **Abstração Multicloud**: Unificar a API para interagir com diferentes serviços de armazenamento, como AWS S3, Azure Blob Storage, Google Cloud Storage e armazenamento local, permitindo a migração ou uso de múltiplos serviços sem mudanças significativas no código-base.
*   **Gerenciamento de Arquivos**: Permitir operações comuns de gerenciamento de arquivos, como leitura, listagem, upload, exclusão e movimentação de arquivos e diretórios entre diferentes ambientes de armazenamento.
*   **Conformidade e Segurança**: Garantir que a transferência de dados e operações de armazenamento estejam em conformidade com as políticas de segurança, como a utilização de conexões criptografadas e o gerenciamento seguro de credenciais.
*   **Extensibilidade**: Facilitar a adição de novos provedores de armazenamento e funcionalidades, mantendo uma interface consistente.

## Operações

*   **Read (****`read`****)**: Leitura de arquivos e streams de dados.
*   **List (****`list`****)**: Listagem de arquivos e diretórios em um bucket ou caminho específico.
*   **Put (****`put`****)**: Upload de arquivos ou streams de dados para o armazenamento.
*   **Delete (****`delete`****)**: Exclusão de arquivos ou diretórios.
*   **Move (****`move`****)**: Movimentação ou renomeação de arquivos e diretórios.
*   **Copy (****`copy`****):** Copiar arquivos ou diretórios
*   **Sync (****`sync`****):** Sincronizar diretórios
*   **Exists (****`exists`****)**: \*\*\*\*Verificar a existência de um arquivo ou diretório
*   **Get Metadata (****`get_metadata`****):** Recuperar metadados de um arquivo ou diretório
*   **Set Metadata (****`set_metadata`****)**: Definir ou atualizar metadados para um arquivo
*   **Get URL (****`get_url`****)**: Obter a URL de acesso direto a um arquivo

## **Escopo**

*   A biblioteca não se destina a ser uma ferramenta de gerenciamento completo de armazenamento de dados, mas sim uma camada de abstração para operações de fluxo de trabalho de dados.
*   Não substituirá as funcionalidades específicas dos clientes SDK para cada plataforma de armazenamento, mas oferecerá um subset de operações comuns necessárias para tarefas de ETL e gestão de dados.

Esta definição serve como uma introdução ao propósito e capacidades da **`storage-tool`**, estabelecendo uma base para detalhar as funcionalidades e a lógica de funcionamento nos próximos pontos

  

# Exemplo das Operações

* * *

### **Read (****`read`****)**

**Descrição**: Lê o conteúdo de um arquivo do armazenamento e o retorna, possivelmente como um objeto de stream.

**Exemplo**:

```python
data = storage.read('path/to/file.txt')
print(data)
```

### **List (****`list`****)**

**Descrição**: Lista todos os arquivos e diretórios em um caminho específico do armazenamento.

**Exemplo**:

```python
files = storage.list('path/to/directory')
for file in files:
    print(file)
```

### **Put (****`put`****)**

**Descrição**: Faz o upload de um arquivo ou stream de dados para um caminho específico no armazenamento.

**Exemplo**:

```python
storage.put('path/to/destination/file.txt', data)
```

### **Delete (****`delete`****)**

**Descrição**: Remove um arquivo ou diretório do armazenamento.

**Exemplo**:

```python
storage.delete('path/to/file.txt')
```

### **Move (****`move`****)**

**Descrição**: Move ou renomeia um arquivo ou diretório dentro do armazenamento.

**Exemplo**:

```python
storage.move('path/to/source/file.txt', 'path/to/destination/file.txt')
```

### **Copy (****`copy`****)**

**Descrição**: Copia arquivos ou diretórios dentro do mesmo armazenamento ou entre diferentes ambientes.

**Exemplo**:

```python
storage.copy('path/to/source/file.txt', 'path/to/destination/file.txt')
```

### **Sync (****`sync`****)**

**Descrição**: Sincroniza diretórios entre diferentes ambientes de armazenamento.

**Exemplo**:

```python
storage.sync('path/to/source/directory', 'path/to/destination/directory')
```

### **Exists (****`exists`****)**

**Descrição**: Verifica se um arquivo ou diretório existe.

**Exemplo**:

```python
if storage.exists('path/to/file.txt'):
    print("File exists")
```

### **Get Metadata (****`get_metadata`****)**

**Descrição**: Obtém metadados associados a um arquivo ou diretório.

**Exemplo**:

```python
metadata = storage.get_metadata('path/to/file.txt')
print(metadata)
```

### **Set Metadata (****`set_metadata`****)**

**Descrição**: Define ou atualiza os metadados de um arquivo ou diretório.

**Exemplo**:

```python
storage.set_metadata('path/to/file.txt', {'key': 'value'})
```

### **Get URL (****`get_url`****)**

**Descrição**: Gera uma URL de acesso direto a um arquivo.

**Exemplo**:

```python
url = storage.get_url('path/to/file.txt')
print(url)
```

  

Estas são descrições e exemplos de alto nível que ilustram o propósito de cada função. A implementação concreta desses métodos precisará lidar com detalhes específicos dos provedores de armazenamento e pode variar consideravelmente dependendo das APIs e SDKs usados.

# Dependências

* * *

## Bibliotecas cliente

| Ambiente | Lib Python |
| ---| --- |
| AWS | boto3 |
| Azure Blob | azure.storage.blob (BlobServiceClient) |
| GCP | [google.cloud.storage](http://google.cloud.storage) |
| Local | os |

  

## Matriz de Conceitos

|  | AWS | Azure | GCP | Local |
| ---| ---| ---| ---| --- |
| Repositório | Bucket | Container | Bucket | Folder |
| Arquivo | Objects | Blobs | Objects | Files |

##   

## Instanciamento e Autenticação

### AWS S3

**Autenticação:**

Para autenticar com AWS S3 será necessário ter: `aws_access_key` , `aws_secret_access_key` e `aws_session_token` .

**Exemplo:**

```python
from storage_tool import Auth, Storage

auth = Auth("S3").authenticator
auth.set_credentials(aws_key, aws_secret, aws_region)

storage = Storage("S3", auth).get_model()


```

###   

### **Azure Blob Storage**

Em desenvolvimento

### **Google Cloud Storage (GCP)**

Em desenvolvimento

  

### **Ambiente Local**

Para armazenamento local, não é necessária autenticação especial, pois o acesso é controlado pelos próprios mecanismos de permissão do sistema de arquivos do sistema operacional em que o código está sendo executado.

**Exemplo**:

```python
from storage_tool import Auth, Storage

auth = Auth("LOCAL").authenticator
storage = Storage("LOCAL", auth).get_model()
```

  

Em cada um desses casos, a biblioteca **`storage-tool`** deverá fornecer uma forma padronizada de configurar a autenticação para que os usuários possam alternar facilmente entre diferentes ambientes de armazenamento sem se preocupar com os detalhes específicos de cada serviço. Isso pode ser feito através de uma interface de configuração unificada que encapsula as diferentes formas de autenticação.

###   

## Como conseguir as credenciais em cada ambiente

### **AWS S3 com** **`boto3`**

Para obter credenciais para a AWS S3:

1. Acesse o [AWS Management Console](https://aws.amazon.com/console/).
2. Navegue até o serviço IAM (Identity and Access Management).
3. Crie um novo usuário IAM ou use um existente.
4. Na seção de permissões do usuário, atribua políticas que concedam acesso ao S3.
5. Na etapa final de criação do usuário, você receberá o **`AWS_ACCESS_KEY_ID`** e o **`AWS_SECRET_ACCESS_KEY`**. Anote-os com segurança.
6. (Opcional) Para um ambiente de produção, é recomendável usar funções IAM para EC2 ou serviços relevantes ao invés de chaves de acesso diretas.

### **Azure Blob Storage com** **`azure-storage-blob`**

Para obter credenciais para o Azure Blob Storage:

1. Acesse o [Azure Portal](https://portal.azure.com/).
2. Vá para "Storage accounts" e selecione ou crie uma nova conta de armazenamento.
3. No painel da conta de armazenamento, encontre a seção "Access keys" ou "Shared access signature".
4. Copie a "Connection string" ou crie uma SAS (Shared Access Signature) conforme necessário.

### **Google Cloud Storage (GCP) com** **`google-cloud-storage`**

Para obter credenciais para o Google Cloud Storage:

1. Acesse o [Google Cloud Console](https://console.cloud.google.com/).
2. Selecione ou crie um projeto.
3. Navegue até "IAM & Admin" > "Service accounts".
4. Crie uma nova conta de serviço ou selecione uma existente.
5. Clique em "Create Key" e escolha o formato JSON. Salve o arquivo de chave JSON em um local seguro.
6. Defina a variável de ambiente **`GOOGLE_APPLICATION_CREDENTIALS`** para o caminho do arquivo JSON de chave de serviço.

### **Ambiente Local**

Para o ambiente local, não são necessárias credenciais de serviços em nuvem. No entanto, você deve garantir que o usuário sob o qual sua aplicação está rodando tenha as permissões adequadas para ler e escrever nos diretórios necessários.

# Modelo Configuração e Arquitetura de Classe

* * *

**Classe Principal (\`\_\_init\_\_.py\`)**

```python
 class Storage:
    def __init__(self, storage_type, authorization) -> None:
        self.storage_type = storage_type
        self.authorization = authorization
        self.storage = None

    def get_model(self):
        if self.storage_type == 'S3':
            self.storage = S3Storage(self.authorization)
            
        elif self.storage_type == 'LOCAL':
            self.storage = LocalStorage()
            
        elif self.storage_type == 'GCP':
            raise NotImplementedError
        elif self.storage_type == 'Azure':
            raise NotImplementedError
        else:
            raise NotImplementedError
        return self.storage
```

  

**Classe Base de Storage**

```python
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

    # ... outras funções ...
```

### **Subclasses para Cada Serviço de Armazenamento**

```python
class S3Storage(BaseStorage, DataProcessor):
    # Define permitted return types
    return_types = [dict, pd.DataFrame, list]

    def __init__(self, Authorization):
        if not isinstance(Authorization, S3Authorization):
            raise Exception('Authorization must be an instance of S3Authorization class')
        
        if not Authorization.test_credentials():
            raise Exception('Invalid credentials')

        self.s3_client = Authorization.client
        self.repository = None

     def read(self, file_path, return_type=pd.DataFrame):
        """
        Read file from S3
        :param file_path: File path
        :param return_type: Return type (dict, pd.DataFrame, list)
        return: File content
        """
        if not self.repository:
            raise Exception('Repository not set')

        try:
            response = self.s3_client.get_object(
                Bucket=self.repository,
                Key=file_path
            )
            file_extension = file_path.split('.')[-1].lower()
            data = self.process_data(response['Body'].read(), file_extension, return_type)
            return data

        except ClientError as e:
            raise Exception(f'Error while reading file: {e}')
        except Exception as e:
            raise Exception(f'Error while reading file: {e}')

# Exemplo similar para Azure, GCP, etc.
```

  

# Estrutura de Pastas e Arquivos

* * *

A estrutura de pastas para a biblioteca **`storage-tool`** deve ser organizada de forma lógica e intuitiva, seguindo as convenções padrão de projetos Python. Abaixo está um exemplo de estrutura de diretórios que você pode usar como ponto de partida:

```php
graphqlCopy code
storage-tool/
│
├── storage_tool/          # Pasta principal da biblioteca
│   ├── __init__.py           # Inicializa o pacote Python
│   ├── base.py               # Classe base de armazenamento
│   ├── s3.py                 # Módulo para AWS S3
│   ├── azure.py              # Módulo para Azure Blob Storage
│   ├── gcp.py                # Módulo para Google Cloud Storage
│   ├── local.py              # Módulo para armazenamento local
│   └── utils.py              # Utilitários e funções auxiliares
│
├── tests/                    # Testes unitários e de integração
│   ├── __init__.py
│   ├── test_base.py
│   ├── test_aws.py
│   ├── test_azure.py
│   ├── test_gcp.py
│   └── test_local.py
│
├── examples/                 # Exemplos de como usar a biblioteca
│   ├── __init__.py
│   ├── aws_example.py
│   ├── azure_example.py
│   ├── gcp_example.py
│   └── local_example.py
│
├── docs/                     # Documentação do projeto
│   ├── conf.py               # Configuração do Sphinx para geração de documentação
│   ├── index.rst
│   └── ...
│
├── .gitignore                # Arquivos e pastas a serem ignorados pelo git
├── LICENSE                   # Licença do projeto
├── README.md                 # README para explicar o projeto, instalação, uso, etc.
├── setup.py                  # Script de instalação para a biblioteca
└── requirements.txt          # Dependências necessárias para o projeto


```

### **Descrição dos Componentes:**

*   **`storage-tool/`**: Diretório principal que contém todo o código-fonte da biblioteca.
*   **`tests/`**: Contém todos os testes automatizados. Os testes são separados em arquivos correspondentes aos módulos que eles estão testando.
*   **`examples/`**: Um lugar para mostrar exemplos concretos de como a biblioteca pode ser utilizada. Isso é útil para novos usuários ou para demonstrar casos de uso específicos.
*   **`docs/`**: Aqui você coloca toda a documentação do projeto, idealmente usando um gerador de documentação como Sphinx.
*   **`.gitignore`**: Este arquivo diz ao Git quais arquivos ou padrões ele deve ignorar.
*   **`LICENSE`**: Um arquivo de texto contendo a licença sob a qual a biblioteca é distribuída.
*   [**`README.md`**](http://README.md): Uma introdução ao projeto, instruções de instalação e uso básico, e qualquer outra informação relevante para os usuários da biblioteca.
*   [**`setup.py`**](http://setup.py): Este script é usado pelo **`setuptools`** para instalar a biblioteca.
*   **`requirements.txt`**: Lista todas as dependências externas necessárias para executar a biblioteca.
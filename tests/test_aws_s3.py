import pytest
import json
from unittest.mock import Mock, patch


from storage_tool.s3 import S3Storage
from storage_tool.s3 import S3Authorization
import pandas as pd

with open('secrets/secret_aws.json') as json_file:
    data = json.load(json_file)
    KEY = data['KEY']
    SECRET = data['SECRET']
    REGION = data['REGION']


def test_s3_authorization():
    # Test Instance
    s3_auth = S3Authorization()
    assert s3_auth.aws_access_key_id == None
    assert s3_auth.aws_secret_access_key == None
    assert s3_auth.region_name == None

    # Test Set Credentials
    s3_auth.set_credentials(KEY, SECRET, REGION)
    assert s3_auth.aws_access_key_id == KEY
    assert s3_auth.aws_secret_access_key == SECRET
    assert s3_auth.region_name == REGION
    assert s3_auth.test_credentials() == True

    # Test Wrong Credentials
    s3_auth.set_credentials('wrong_aws_access_key_id', 'wrong_aws_secret_access_key', 'us-east-1')
    assert s3_auth.test_credentials() == False
    assert s3_auth.client != None

def test_set_repository_existente(monkeypatch):
    # Simular a resposta da função list_repositories
    repositories_mock = [{"repository": "Repo1"}, {"repository": "Repo2"}]
    monkeypatch.setattr('storage_tool.s3.S3Storage.list_repositories', lambda _: repositories_mock)

    # Instanciar a classe
    auth = S3Authorization()
    auth.set_credentials(KEY, SECRET, REGION)
    obj = S3Storage(auth)

    # Testar com um repositório que existe
    obj.set_repository("Repo1")
    assert obj.repository == "Repo1"

def test_set_repository_inexistente(monkeypatch):
    # Simular a resposta da função list_repositories
    repositories_mock = [{"repository": "Repo1"}, {"repository": "Repo2"}]
    monkeypatch.setattr('storage_tool.s3.S3Storage.list_repositories', lambda _: repositories_mock)

    # Instanciar a classe
    auth = S3Authorization()
    auth.set_credentials(KEY, SECRET, REGION)
    obj = S3Storage(auth)

    # Testar com um repositório que não existe
    with pytest.raises(Exception):
        obj.set_repository("RepoInexistente")

def test_create_repository():
    # Criar um mock para o cliente S3
    s3_client_mock = Mock()
    s3_client_mock.create_bucket.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

    # Substituir a função que cria o cliente S3 para retornar o mock
    with patch('boto3.client', return_value=s3_client_mock):
        auth = S3Authorization()
        auth.set_credentials(KEY, SECRET, REGION)
        obj = S3Storage(auth)

        # Chamar o método create_repository
        response = obj.create_repository("meu-novo-bucket")

        # Verificar se o método create_bucket foi chamado com os parâmetros corretos
        s3_client_mock.create_bucket.assert_called_with(Bucket="meu-novo-bucket")

        # Verificar a resposta
        assert response == {'ResponseMetadata': {'HTTPStatusCode': 200}}

def test_list_repositories_ok():
    # Criar um mock para o cliente S3
    s3_client_mock = Mock()
    s3_client_mock.list_buckets.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 200},
        'Buckets': [{'Name': 'bucket1', 'CreationDate': '2023-01-01'}, 
                    {'Name': 'bucket2', 'CreationDate': '2023-01-02'}]
    }

    # Substituir a função que cria o cliente S3 para retornar o mock
    with patch('boto3.client', return_value=s3_client_mock):
        auth = S3Authorization()
        auth.set_credentials(KEY, SECRET, REGION)
        obj = S3Storage(auth)

        # Chamar o método list_repositories
        result = obj.list_repositories()

        # Verificar se o método list_buckets foi chamado
        s3_client_mock.list_buckets.assert_called()

        # Verificar a resposta
        assert result == [{'repository': 'bucket1', 'created_at': '2023-01-01'},
                          {'repository': 'bucket2', 'created_at': '2023-01-02'}]

def test_list_repositories_failure():
    # Criar um mock para o cliente S3
    s3_client_mock = Mock()
    s3_client_mock.list_buckets.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 500}
    }

    # Substituir a função que cria o cliente S3 para retornar o mock
    with patch('boto3.client', return_value=s3_client_mock):
        auth = S3Authorization()
        auth.set_credentials(KEY, SECRET, REGION)
        obj = S3Storage(auth)

        # Chamar o método list_repositories e verificar se uma exceção é levantada
        with pytest.raises(Exception) as excinfo:
            obj.list_repositories()
        assert 'Error while listing repositories' in str(excinfo.value)
    
def test_list_files_ok():
    # Criar um mock para o cliente S3
    s3_client_mock = Mock()
    s3_client_mock.list_objects_v2.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 200},
        'Contents': [{'Key': 'file1.txt', 'Size': 123, 'LastModified': '2023-01-01'},
                     {'Key': 'file2.txt', 'Size': 456, 'LastModified': '2023-01-02'}]
    }

    s3_client_mock.list_buckets.return_value = {
        'ResponseMetadata': {'HTTPStatusCode': 200},
        'Buckets': [{'Name': 'bucket1', 'CreationDate': '2023-01-01'}]
    }
    
    # Substituir a função que cria o cliente S3 para retornar o mock
    with patch('boto3.client', return_value=s3_client_mock):
        auth = S3Authorization()
        auth.set_credentials(KEY, SECRET, REGION)
        obj = S3Storage(auth)
        obj.set_repository('bucket1')

        # Chamar o método list com um caminho específico
        result = obj.list('path/to/files/')

        # Verificar se o método list_objects_v2 foi chamado com os parâmetros corretos
        s3_client_mock.list_objects_v2.assert_called_with(Bucket='bucket1', Prefix='path/to/files/')

        # Verificar a resposta
        assert result == [{'file': 'file1.txt', 'size': 123, 'last_modified': '2023-01-01'},
                          {'file': 'file2.txt', 'size': 456, 'last_modified': '2023-01-02'}]

def test_list_files_no_repository_set():
    # Criar um mock para o cliente S3
    s3_client_mock = Mock()

    # Substituir a função que cria o cliente S3 para retornar o mock
    with patch('boto3.client', return_value=s3_client_mock):
        auth = S3Authorization()
        auth.set_credentials(KEY, SECRET, REGION)
        obj = S3Storage(auth)

        # Chamar o método list sem definir um repositório e verificar se uma exceção é levantada
        with pytest.raises(Exception) as excinfo:
            obj.list('path/to/files/')
        assert 'Repository not set' in str(excinfo.value)

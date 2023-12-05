import os
import pytest
from dotenv import load_dotenv
from datetime import datetime

from storage_tool import Storage, Auth

@pytest.fixture
def azure_credentials():
    # Load environment variables from .env.testing
    load_dotenv('.env.testing')
    return {
        "azure_storage_connection_string": os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        "default_container": os.getenv("AZURE_DEFAULT_STORAGE_CONTAINER"),
        "second_container": os.getenv("AZURE_SECOND_STORAGE_CONTAINER"),
        "storage_type": 'Azure',
    }

@pytest.fixture
def get_storage(azure_credentials):
    try:
        auth = Auth(azure_credentials["storage_type"]).authenticator
        auth.set_credentials(azure_credentials["azure_storage_connection_string"])

        storage = Storage(azure_credentials["storage_type"], auth).get_model()

        return {
            "storage": storage,
        }

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_azure_connection(azure_credentials):

    try:
        auth = Auth(azure_credentials["storage_type"]).authenticator
        auth.set_credentials(azure_credentials["azure_storage_connection_string"])

        assert auth.test_credentials()

        storage = Storage(azure_credentials["storage_type"], auth).get_model()

        assert storage.client is not None

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_creating_containers(azure_credentials, get_storage):

    try:

        storage = get_storage['storage']
        delete_all_containers(client= storage.client)

        storage.create_repository(repository=azure_credentials['default_container'])
        storage.create_repository(repository=azure_credentials['second_container'])

        containers = storage.list_repositories()

        assert len(containers) == 2

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_listing_containers(azure_credentials, get_storage):

    try:

        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])

        containers = storage.list_repositories()

        assert len(containers) == 2

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_exists(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        filename = datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'

        assert storage.exists(filename) is False

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_put_file_on_container(azure_credentials, get_storage):

    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        filename = datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)

        assert storage.exists(filename) is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_put_raises_exception(azure_credentials, get_storage):
    filename = datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'

    with pytest.raises(Exception, match=f'Error while writing file: {filename}'):
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])

        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)
        storage.put(file_path=filename, content=data_fake)


def test_listing_files(azure_credentials, get_storage):

    try:
        storage = get_storage['storage']

        storage.set_repository(repository= azure_credentials['default_container'])

        # print(storage.list())
        delete_all_files_in_container(
            container_client= storage.client.get_container_client(container= azure_credentials['default_container'])
        )
        # print(storage.list())

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

        listing_folderA = storage.list(path='folderA/')

        assert len(listing_folderA) == 3
        found_subfolder = any(item['object'] == 'subfolderA/' for item in listing_folderA)
        assert found_subfolder, "'subfolderA/' not found in data"

        listing_subfolderA = storage.list(path='subfolderA/')

        assert len(listing_subfolderA) == 2

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_read_file(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)

        assert storage.exists(filename) is True

        data = storage.read(file_path=filename, return_type=dict)

        supposed_dict_fake = {'col1': {0: 1, 1: 1}, 'col2': {0: 2, 1: 2}}

        assert data == supposed_dict_fake

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_delete_file(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)

        assert storage.exists(filename) is True

        storage.delete(file_path=filename)

        assert storage.exists(filename) is False

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_get_metadata_file(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)

        assert storage.exists(filename) is True

        metadata = storage.get_metadata(file_path=filename)

        assert metadata['name'] == filename
        assert metadata['container'] == azure_credentials['default_container']

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_get_url(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)

        assert storage.exists(filename) is True

        url = storage.get_file_url(file_path=filename)

        local_url = f'http://127.0.0.1:10000/devstoreaccount1/test-container/{filename}'
        assert url == local_url

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_moving(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        dst_file = f'moving/{filename}'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)
        assert storage.exists(filename) is True

        storage.move(filename, dst_file)

        assert storage.exists(filename) is False
        assert storage.exists(dst_file) is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_moving_between_containers(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        dst_file = f'moving/{filename}'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)
        assert storage.exists(filename) is True

        storage.move_between_repositories(
            src_repository=azure_credentials['default_container'],
            src_path=filename,
            dest_repository=azure_credentials['second_container'],
            dest_path=filename
        )

        assert storage.exists(filename) is False

        storage.set_repository(repository=azure_credentials['second_container'])
        assert storage.exists(filename) is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_copying(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        dst_file = f'moving/{filename}'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)
        assert storage.exists(filename) is True

        storage.copy(filename, dst_file)

        assert storage.exists(filename) is True
        assert storage.exists(dst_file) is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_copying_between_containers(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        dst_file = f'moving/{filename}'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)
        assert storage.exists(filename) is True

        storage.copy_between_repositories(
            src_repository=azure_credentials['default_container'],
            src_path=filename,
            dest_repository=azure_credentials['second_container'],
            dest_path=filename
        )

        assert storage.exists(filename) is True

        storage.set_repository(repository=azure_credentials['second_container'])
        assert storage.exists(filename) is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_syncying(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        src_path = f'source/'
        dst_path = f'syncing/'
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=f'{src_path}{filename}', content=data_fake)
        assert storage.exists(f'{src_path}{filename}') is True

        storage.sync(src_path, dst_path)

        assert storage.exists(f'{dst_path}{filename}') is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_syncying_between_containers(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_repository(repository=azure_credentials['default_container'])
        current_time = datetime.now()
        src_path = f'source/'
        dest_path = f'syncing/'
        filename = current_time.strftime("%Y%m%d%H%M%S%f")[:-3] + '.csv'
        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=f'{src_path}{filename}', content=data_fake)
        assert storage.exists(f'{src_path}{filename}') is True

        storage.sync_between_repositories(
            src_repository=azure_credentials['default_container'],
            src_path=src_path,
            dest_repository=azure_credentials['second_container'],
            dest_path=dest_path
        )

        storage.set_repository(repository=azure_credentials['second_container'])
        assert storage.exists(f'{dest_path}{filename}') is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def delete_all_files_in_container(container_client):
    # List blobs in the container
    blob_list = container_client.list_blobs()

    # Delete each blob in the container
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob.name)
        blob_client.delete_blob()


def delete_all_containers(client):
    # List all containers in the storage account
    containers = client.list_containers()

    # Delete each container
    for container in containers:
        container_client = client.get_container_client(container['name'])
        container_client.delete_container()

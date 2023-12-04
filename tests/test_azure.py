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


def test_listing_containers(azure_credentials, get_storage):

    try:

        storage = get_storage['storage']
        storage.set_container(container=azure_credentials['default_container'])

        containers = storage.list_repositories()

        assert len(containers) == 2

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_exists(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_container(container=azure_credentials['default_container'])
        filename = datetime.now().strftime("%Y%m%d%H%M%S") + '.csv'

        assert storage.exists(filename) is False

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_put_file_on_container(azure_credentials, get_storage):

    try:
        storage = get_storage['storage']
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])

        data_fake = [{'col1': 1, 'col2': 2},{'col1': 1, 'col2': 2}]

        storage.put(file_path=filename, content=data_fake)
        storage.put(file_path=filename, content=data_fake)


def test_read_file(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])
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

        storage.set_container(container=azure_credentials['second_container'])
        assert storage.exists(filename) is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_copying(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])
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

        storage.set_container(container=azure_credentials['second_container'])
        assert storage.exists(filename) is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")


def test_syncying(azure_credentials, get_storage):
    try:
        storage = get_storage['storage']
        storage.set_container(container=azure_credentials['default_container'])
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
        storage.set_container(container=azure_credentials['default_container'])
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

        storage.set_container(container=azure_credentials['second_container'])
        assert storage.exists(f'{dest_path}{filename}') is True

    except Exception as e:
        pytest.fail(f"Azure Storage connection test failed: {e}")

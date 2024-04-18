import Storage, Auth

STORAGE_TYPE = "GCS"

auth = Auth(STORAGE_TYPE).authenticator
auth.set_credentials(project_id="project_id",
                     private_key_id="private_key_id",
                     private_key="private_key",
                     client_email="client_email",
                     client_id="client_id")

storage = Storage(STORAGE_TYPE, auth).get_model()
my_bucket_name = "my_bucket_name"
storage.set_repository(repository=my_bucket_name)

path = "test/first_file.txt"
data = "Hello World!"
storage.put(path, data)

content = storage.read(path)

assert content == data
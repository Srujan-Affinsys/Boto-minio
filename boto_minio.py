import boto3
# Boto3
import botocore
from base64 import b64decode
import uuid
from pathlib import Path
from botocore.exceptions import ClientError
import os
STORAGE_SERVICE = 's3'
ACCESS_KEY = 'inm_img'
SECRET_KEY = 'buddybuddy'
VERSION = 's3v4'
HOST = 'http://127.0.0.1:9000/'


# check if bucket exists on minio               Done
# check if files exists on minio                Done
# get the content of files present on the mino  Done
class BotoMinio:
    def __init__(self, storage=None, access_key=None, secret_key=None, host=None, version=None):

        self.storage = storage or STORAGE_SERVICE
        self.access_key = access_key or ACCESS_KEY
        self.secret_key = secret_key or SECRET_KEY
        self.hostname = host or HOST
        self.version = version or VERSION

        self.resource = boto3.resource(storage,
                                       endpoint_url=self.hostname,
                                       config=boto3.session.Config(
                                           signature_version=self.version),
                                       aws_access_key_id=self.access_key,  
                                       aws_secret_access_key=self.secret_key)

        self.client = boto3.client(storage,
                                   endpoint_url=HOST,
                                   config=boto3.session.Config(
                                       signature_version=self.version),
                                   aws_access_key_id=self.access_key,
                                   aws_secret_access_key=self.secret_key)

    def post_data(self, bucket_name, data, object_name):
        try:
            object = self.resource.Object(bucket_name, object_name)
            response = object.put(Body=data)  # todo return of .put()
        except ClientError as error:
            if error.response['ResponseMetadata']['HTTPStatusCode']==404:
                return False
        return response['ResponseMetadata']['HTTPStatusCode'] == 200
        # rt link, object_path, dict


    def check_local_file_exist(self,file_name):
        if os.path.exists(file_name):
            return True
        return False
    def post_file(self, bucket_name, file_name, object_name=None):
        if object_name is None:
            object_name = str(uuid.uuid4())+file_name

        if self.check_bucket_exist(bucket_name) and self.check_local_file_exist(file_name):
            self.client.upload_file(file_name, bucket_name, object_name)
            return True
        return False  
        
    def get_link(self, bucket_name, object_path, time):
     
            if self.check_bucket_exist(bucket_name) and self.check_object_exist(bucket_name,object_path):
                
                response = self.client.generate_presigned_url('get_object',
                                                        Params={
                                                            'Bucket': bucket_name,
                                                            'Key': object_path
                                                        },
                                                        ExpiresIn=time)
                return response
            else:
                return None
            

    def create_new_bucket(self, bucket_name):

        if not self.check_bucket_exist(bucket_name):
            self.client.create_bucket(Bucket=bucket_name)
            return True
        
        return False
        

    def delete_object_file(self, bucket_name, file_path):#todo 
        """
        !!!!Even if the object does not exist it doesn't show up any error!!!!
        """
        try:
            response=self.resource.Object(bucket_name, file_path).delete()
        except ClientError:
            return False
        return response['ResponseMetadata']['HTTPStatusCode'] == 204
        

    def del_bucket(self, bucket_name):

        #self.client.delete_bucket(Bucket=bucket_name) "only empty buckets"


        if self.check_bucket_exist(bucket_name):

            bucket = self.resource.Bucket(bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
            return True
        return False

    def post_file_get_link(self, bucket_name, file_name, object_name):

        if self.check_bucket_exist(bucket_name):
            self.post_file(bucket_name, file_name, object_name)
            return HOST + bucket_name + '/' + object_name
        return None

    def read_bytes(self, bucket_name, object_name, file_name):

        with open(file_name, 'rb') as fp:
            byte = fp.read()

            self.post_data(bucket_name, byte, object_name)

    def upload_base64(self, bucket_name, base64_data: str, file_name, subfolder='', full_object_path=None,
                      unqiue_filename=False):
        object_path = Path(file_name)

        if full_object_path:
            object_path = Path(full_object_path)
        elif subfolder:
            object_path = Path(subfolder) / Path(file_name)

        bytes_data: bytes = b64decode(base64_data)

        self.post_data(bucket_name, bytes_data, object_path.as_posix())

    def check_bucket_exist(self, bucket_name):  
        
        try:
             response = self.client.head_bucket(
            Bucket=bucket_name,
        )
  
        
        except ClientError:
            return False
        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def check_object_exist(self, bucket_name, object_path) -> bool:  # Todo return types and object types

        try:
            bucket = self.resource.Bucket(bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                
                return False
        return object_path in [object.key for object in bucket.objects.all()]

    def read_object_content(self, bucket_name, object_name):

        bucket = self.resource.Bucket(bucket_name)
        for obj in bucket.objects.all():
            if obj.key == object_name:
                return obj.get()['Body'].read()


bm_obj = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)

byt_data = bytes(10)
# OR text_data_byt=b'\x00\x00\x00\x00\x00' value for 5
str_data = 'BYEEE good niGhT'  # strings
bucket_name = 'imagesss'
object_name_byt = 'greetings_byt.txt'
object_name_str = 'greetings_str.txt'
#bm_obj.post_data(bucket_name, byt_data, object_name_byt)
#print(bm_obj.post_data(bucket_name, str_data, object_name_str))

bucket_name = 'images'
object_path = 'bottle_iddn_minio_2'
expire_time = 3600
#print(bm_obj.get_link(bucket_name,object_path,expire_time))

bucket_name = 'images'
file_name = 'india.png'
object_name = 'india_in_minio_2'
#bm_obj.post_file(bucket_name,file_name,object_name)

bucket_name = 'images'
file_name = 'todo.txt'
object_name = 'india_in_minio_3'
#print(bm_obj.post_file(bucket_name, file_name,object_name))

bucket_name = 'mybucket999'
#print(bm_obj.create_new_bucket(bucket_name))

bucket_name = 'images'
file_path = 'me/india_in_minio.png'
#print(bm_obj.delete_object_file(bucket_name,file_path))


bucket_name = 'mybucket8'
#print(bm_obj.del_bucket(bucket_name))


bucket_name = 'images'
file_name = 'india.png'
object_name = 'me/india_in_minio.png'
# print(bm_obj.post_file_get_link(bucket_name, file_name, object_name))

bucket_name = 'mybucket'
file_name = 'bytes.txt'
file_in = 'nature.jpeg'
# bm_obj.read_bytes(bucket_name, file_name, file_in)


bucket_name = 'mybucket3'
base64_data = 'c3J1amFu'
file_name = 'base64_file'
subfolder = 'me'
full_object_path = None
unqiue_filename = False
# bm_obj.upload_base64(bucket_name,base64_data,file_name,subfolder,full_object_path,unqiue_filename)


bucket_name = 'mybucketaa2'
#print(bm_obj.check_bucket_exist(bucket_name))

bucket_name = 'imagesn'
file_path = 'me/image.png'
#print(bm_obj.check_object_exist(bucket_name,file_path))

bucket_name = 'images'
object_name = 'greetings_str.txt'
#print(bm_obj.read_object_content(bucket_name, object_name))

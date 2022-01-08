import boto3
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
        """Posts the data into the specified object path"""
        try:
            object = self.resource.Object(bucket_name, object_name)
            response = object.put(Body=data)  # todo return of .put()
        except ClientError as error:
            if error.response['ResponseMetadata']['HTTPStatusCode']==404:
                return False
        return response['ResponseMetadata']['HTTPStatusCode'] == 200
  
    
    def post_data_get_link(self, bucket_name, data, object_name):
        """Posts the data into the specified object path and gets the link of the object"""
        if (self.check_bucket_exist(bucket_name)):
            self.resource.Object(bucket_name, object_name).put(Body=data)
              
            return HOST + bucket_name + '/' + object_name

    def check_local_file_exist(self,file_name):
        """Helper method to verify if the specified local file exists in the local directory"""
        if os.path.exists(file_name):
            return True
        return False

    def post_file(self, bucket_name, file_name, object_name=None):
        """Uploads the file in to the minio"""
        if object_name is None:
            object_name = str(uuid.uuid4())+file_name

        if self.check_bucket_exist(bucket_name) and self.check_local_file_exist(file_name):
            self.client.upload_file(file_name, bucket_name, object_name)
            return True
        return False  
        
    def get_link(self, bucket_name, object_path, time):
        """Gives the download link of the specified object"""
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
        """Creates a bucket with speciefied bucket name"""
        if not self.check_bucket_exist(bucket_name):
            self.client.create_bucket(Bucket=bucket_name)
            return True
        
        return False

    def create_new_uuid_bucket(self):
        """Creates a bucket with uuid as bucket_name"""
        response=self.client.create_bucket(Bucket=str(uuid.uuid4()))
        return response['ResponseMetadata']['HTTPHeaders']['location'][1:]
      
    
  
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

        """Deletes the bucket with the specified bucket name"""


        if self.check_bucket_exist(bucket_name):

            bucket = self.resource.Bucket(bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
            return True
        return False

    def del_bucket_uuid(self, bucket_name):
        """Deletes the bucket with uuid bucket name"""

        if self.check_bucket_exist(bucket_name):

            bucket = self.resource.Bucket(bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
            
        


    def post_file_get_link(self, bucket_name, file_name, object_name):
        """Gives the download link after uploading the file"""
        if self.check_bucket_exist(bucket_name) and self.check_local_file_exist(file_name):
            self.post_file(bucket_name, file_name, object_name)
            return HOST + bucket_name + '/' + object_name
        return None

    def read_bytes(self, bucket_name, object_name, file_name):
        """Read the bytes from the specifief file_name and posts it as an object"""
        with open(file_name, 'rb') as fp:
            byte = fp.read()

            self.post_data(bucket_name, byte, object_name)

    def upload_base64(self, bucket_name, base64_data: str, file_name, subfolder='', full_object_path=None,
                      unqiue_filename=False):

        """Uploads base64 encoded string"""
        object_path = Path(file_name)

        if full_object_path:
            object_path = Path(full_object_path)
        elif subfolder:
            object_path = Path(subfolder) / Path(file_name)

        bytes_data: bytes = b64decode(base64_data)

        self.post_data(bucket_name, bytes_data, object_path.as_posix())

    def check_bucket_exist(self, bucket_name):  
        """Checks weather the bucket exists or not"""
        try:
             response = self.client.head_bucket(
            Bucket=bucket_name,
        )
  
        
        except ClientError:
            return False
        return response['ResponseMetadata']['HTTPStatusCode'] == 200

    def check_object_exist(self, bucket_name, object_path) -> bool:  
        """Checks weather the Object exists or not"""

        try:
            bucket = self.resource.Bucket(bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                
                return False
        return object_path in [object.key for object in bucket.objects.all()]

    def read_object_content_bytes(self, bucket_name, object_name):
        """Reads the object contents and returns as bytes"""
        if self.check_bucket_exist(bucket_name):
            bucket = self.resource.Bucket(bucket_name)
            for obj in bucket.objects.all():
                if obj.key == object_name:
                    data_read= obj.get()['Body'].read()
                    return data_read

    def read_object_content_string(self, bucket_name, object_name):
        """Reads the object contents and returns as string"""
        if self.check_bucket_exist(bucket_name):
            bucket = self.resource.Bucket(bucket_name)
            for obj in bucket.objects.all():
                if obj.key == object_name:
                    data_read= obj.get()['Body'].read()
                    return data_read.decode()
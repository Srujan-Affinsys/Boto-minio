import unittest
import uuid 
from boto_minio import BotoMinio, STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY


class TestFile(unittest.TestCase):

    def test_get_link(self):
        """check if we can get existing object link"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        minio.post_file('images','input_file.txt','images/input_file_in_minio.txt')
        self.assertIsNotNone(minio.get_link('images','images/input_file_in_minio.txt',3800))

    def test_get_link_bucket_not_existing(self):
        """check if we can't get object link bucket not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertIsNone(minio.get_link(str(uuid.uuid4()),str(uuid.uuid4()),3800))
    
    def test_post_file_get_link(self):
        """check if we can get object link after uploading the file"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertIsNotNone(minio.post_file_get_link('images','input_file.txt','images/input_file_in_minio.txt'))

    def test_post_file_get_link_bucket_not_existing(self):
        """check if we can't get object link after uploading the file, bucket not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertIsNone(minio.post_file_get_link(str(uuid.uuid4()),'input_file.txt','images/input_file_in_minio.txt'))

    def test_post_file_get_link_file_path_not_existing(self):
        """check if we can't get object link after uploading the file ,filepath not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertIsNone(minio.post_file_get_link('images',str(uuid.uuid4()),'demo_minio.txt'))

    
    def test_post_data_get_link(self):
        """check if we can get object link after uploading the text"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        data="sss"
        self.assertIsNotNone(minio.post_data_get_link('images',data,'demo_minio.txt'))

class UuidBuckets(unittest.TestCase):
    def test_create_new_bucket_existing_name(self): 
        """Check if we can't create a bucket existing previously in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)  
        bucket_name=minio.create_new_uuid_bucket()  
        self.assertFalse(minio.create_new_bucket(bucket_name))
        minio.del_bucket_uuid(bucket_name)

    def test_del_bucket(self):
        """Check if we can delete a bucket in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        self.assertTrue(minio.del_bucket(bucket_name))
        minio.del_bucket_uuid(bucket_name)

    def test_del_bucket_not_existing(self): 
        """Check if we can't delete a bucket not existing in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.del_bucket(str(uuid.uuid4())))

    def test_check_bucket_exist(self):
        """Check if a bucket existing in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket() 
        self.assertTrue(minio.check_bucket_exist(bucket_name))
        minio.del_bucket_uuid(bucket_name)
    
    def test_check_bucket_exist_not_existing(self):
        """Check if a bucket not existing in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.check_bucket_exist(str(uuid.uuid4())))

    def test_upload_text_string_uuid(self):
        """check if we can upload text into minio using UUID"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        data  = 'UUUIID srujan\'s data'
        self.assertTrue(minio.post_data(bucket_name,data,'uuid_test.txt'))
        self.assertEqual(minio.read_object_content_string(bucket_name,'uuid_test.txt'),data,data)
        minio.del_bucket_uuid(bucket_name)

    def test_upload_text_string_given_object_folder_uuid(self):
        """check if we can upload text into minio inside a folder"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        data  = 'UUUIID srujan\'s data'
        self.assertTrue(minio.post_data(bucket_name,data,'subfolder/uuid_test.txt'))
        self.assertEqual(minio.read_object_content_string(bucket_name,'subfolder/uuid_test.txt'),data,data)
        minio.del_bucket_uuid(bucket_name)
    
    def test_upload_text_bucket_not_existing_uuid(self):
        """check if we can't upload text into minio bucket not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.post_data(str(uuid.uuid4()),'testing','images/test_text.txt'))

    def test_upload_text_bytes_uuid(self):
        """check if we can upload text into minio"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        data  = b'UUUIID srujan\'s data'
        self.assertTrue(minio.post_data(bucket_name,data,'uuid_test.txt'))
        self.assertEqual(minio.read_object_content_bytes(bucket_name,'uuid_test.txt'),data,data)
        minio.del_bucket_uuid(bucket_name)

    def test_upload_text_bytes_given_object_folder_uuid(self):
        """check if we can upload text into minio inside a folder"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        data  = b'UUUIID srujan\'s data'
        self.assertTrue(minio.post_data(bucket_name,data,'subfolder/uuid_test.txt'))
        self.assertEqual(minio.read_object_content_bytes(bucket_name,'subfolder/uuid_test.txt'),data,data)
        minio.del_bucket_uuid(bucket_name)

    def test_upload_bytes_bucket_not_existing_uuid(self):
        """check if we can't upload bytes into minio bucket not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.post_data(str(uuid.uuid4()),bytes(10),'images/test_bytes_minio.txt'))

    def test_upload_text_file_uuid(self):
        """check if we can upload file into minio"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()  
        self.assertTrue(minio.post_file(bucket_name,'input_file.txt','input_file_in_minio.txt'))     
        minio.del_bucket_uuid(bucket_name)

    def test_upload_text_file_uuid_given_object_folder_uuid(self):
        """check if we can upload file into minio inside a folder"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()  
        self.assertTrue(minio.post_file(bucket_name,'input_file.txt','images/input_file_in_minio.txt'))     
        minio.del_bucket_uuid(bucket_name)


    def test_upload_text_file_uuid_object_name_not_given(self):
        """check if we can upload text file into minio object name not given set uuid as object name"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        self.assertTrue(minio.post_file(bucket_name,'input_file.txt'))
        minio.del_bucket_uuid(bucket_name)

    def test_upload_text_file_file_path_not_existing_uuid(self):
        """check if we can upload file into minio"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket() 
        self.assertFalse(minio.post_file(bucket_name,str(uuid.uuid4()),'commands_in_minio'))       
        minio.del_bucket_uuid(bucket_name)

    def test_upload_text_file_bucket_not_existing(self):
        """check if we can't upload text file into minio bucket not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.post_file(str(uuid.uuid4()),'input_file.txt','images/input_file_in_minio.txt'))

    def test_upload_text_file_file_path_not_existing(self):
        """check if we can't upload text file into minio filepath not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        self.assertFalse(minio.post_file(bucket_name,str(uuid.uuid4()),'images/input_file_in_minio.txt'))
        minio.del_bucket_uuid(bucket_name)

    def test_delete_object(self):
        """check if we can delete object in minio"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        bucket_name=minio.create_new_uuid_bucket()
        data  = 'UUUIID srujan\'s data'
        minio.post_data(bucket_name,data,'images/input_file_in_minio.txt')
        self.assertTrue(minio.delete_object_file(bucket_name,'images/input_file_in_minio.txt'))
        minio.del_bucket_uuid(bucket_name)

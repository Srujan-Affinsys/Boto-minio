import unittest
import uuid 
from boto_minio import BotoMinio, STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY
#13 methods
# buckets exists or not




class TestBucket(unittest.TestCase):
    def test_create_new_bucket(self):
        """Check if we can create a bucket in mino"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        #self.assertTrue(minio.create_new_bucket(str(uuid.uuid4())))

    def test_create_new_bucket_existing_name(self): 
        """Check if we can't create a bucket existing previously in mino"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)    
        self.assertFalse(minio.create_new_bucket('images'))

    def test_del_bucket(self):
        """Check if we can delete a bucket in mino"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        #self.assertTrue(minio.del_bucket('mybucket3'))

    def test_del_bucket_not_existing(self): 
        """Check if we can't delete a bucket not existing in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.del_bucket(str(uuid.uuid4())))

    def test_check_bucket_exist(self):
        """Check if a bucket existing in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertTrue(minio.check_bucket_exist('mybucket'))

    def test_check_bucket_exist_not_existing(self):
        """Check if a bucket not existing in mino"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.check_bucket_exist(str(uuid.uuid4()))) #not existing

class TestFile(unittest.TestCase):

    def test_upload_text(self):
        """check if we can upload text into minio"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        # bucket_name/location = create_ bucket()

        data  = b'srujan\'s data'
        self.assertTrue(minio.post_data('images',data,'test_text.txt'))

        self.assertEqual( minio.read_object_content_bytes('images','test_text.txt'),data,data)
        
        # delete_bucket(bucket_name)
        
    
    def test_upload_text_bucket_not_existing(self):
        """check if we can't upload text into minio bucket not existing"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.post_data(str(uuid.uuid4()),'testing','test_text.txt'))


    def test_upload_bytes(self):
        """check if we can upload bytes into minio"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        data=b'minio'
        self.assertTrue(minio.post_data('images',data,'test_bytes_minio.txt'))
        self.assertEqual( minio.read_object_content_bytes('images','test_bytes_minio.txt'),data,data)


    def test_upload_bytes_bucket_not_existing(self):
        """check if we can upload bytes into minio"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.post_data(str(uuid.uuid4()),bytes(10),'test_bytes'))

    def test_upload_text_file(self):
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertTrue(minio.post_file('images','commands.txt','commands_in_minio'))
    
    def test_upload_text_file_bucket_not_existing(self):
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.post_file(str(uuid.uuid4()),'commands.txt','commands_in_minio'))
    
    def test_upload_text_file_object_name_not_given(self):
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertTrue(minio.post_file('images','commands.txt'))
       
    def test_upload_text_file_file_path_not_existing(self):
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertFalse(minio.post_file('images',str(uuid.uuid4()),'commands_in_minio'))
       
       
    def test_delete_object(self):
        """check if we can delete object in minio"""
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertTrue(minio.delete_object_file('images','me/image.png'))

    def test_get_link(self):
       
        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertIsNotNone(minio.get_link('images','me/base64_file',3800))

    def test_get_link_bucket_not_existing(self):

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
        self.assertIsNone(minio.get_link(str(uuid.uuid4()),'me/base64_file',3800))
    
    def test_post_file_get_link(self):
            minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
            self.assertIsNotNone(minio.post_file_get_link('mybucket999','demo.txt','demo_minio.txt'))

    def test_post_file_get_link_bucket_not_existing(self):
            minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
            self.assertIsNone(minio.post_file_get_link(str(uuid.uuid4()),'demo.txt','demo_minio.txt'))

    def test_post_file_get_link_file_path_not_existing(self):
            minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
            self.assertIsNone(minio.post_file_get_link('mybucket999',str(uuid.uuid4()),'demo_minio.txt'))

    
    def test_post_data_get_link(self):
            minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)
            data="sss"
            self.assertIsNotNone(minio.post_data_get_link('mybucket999',data,'demo_minio.txt'))

class TestFileUuidBuckets(unittest.TestCase):
    def test_upload_text_string_uuid(self):
        """check if we can upload text into minio"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)

        bucket_name=minio.create_new_uuid_bucket()
        data  = b'UUUIID srujan\'s data'
        self.assertTrue(minio.post_data(bucket_name,data,'uuid_test.txt'))

        self.assertEqual(minio.read_object_content_bytes(bucket_name,'uuid_test.txt'),data,data)
        
        minio.del_bucket_uuid(bucket_name)
        
    def test_upload_text_bytes_uuid(self):
        """check if we can upload text into minio"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)

        bucket_name=minio.create_new_uuid_bucket()
        data  = 'UUUIID srujan\'s data'
        self.assertTrue(minio.post_data(bucket_name,data,'uuid_test.txt'))

        self.assertEqual(minio.read_object_content_string(bucket_name,'uuid_test.txt'),data,data)
        
        minio.del_bucket_uuid(bucket_name)


    def test_upload_text_file_uuid(self):
        """check if we can upload file into minio"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)

        bucket_name=minio.create_new_uuid_bucket()
  
        self.assertTrue(minio.post_file(bucket_name,'commands.txt','commands_in_minio'))
       
        minio.del_bucket_uuid(bucket_name)
        


    def test_upload_text_file_file_path_not_existing_uuid(self):
        """check if we can upload file into minio"""

        minio = BotoMinio(STORAGE_SERVICE, ACCESS_KEY, SECRET_KEY)

        bucket_name=minio.create_new_uuid_bucket()
  
        self.assertFalse(minio.post_file(bucket_name,str(uuid.uuid4()),'commands_in_minio'))
       
        minio.del_bucket_uuid(bucket_name)
        

import boto3
from os import path, listdir
from io import StringIO
import numpy as np
import pandas as pd

class Workbench:
    def __init__(self, bucket, data_dir=None):
        self.data_dir = data_dir if data_dir is None else self.set_data_directory(data_dir)
        self.s3 = boto3.resource('s3', use_ssl=False)
        self.bucket = self.s3.Bucket(bucket)
        self.bucket_name = bucket

    def set_data_directory(self, data_dir):
        if not path.exists(data_dir):
            raise Exception("Path {p} does not exisit".format(p=data_dir))
        else:
            self.data_dir = data_dir
        return

    def get_all_objects(self, only_csv=True):
        all_objects = self.bucket.objects.all()
        matching_objects = []
        for object in all_objects:
            o_name = object.key
            if only_csv:
                if o_name.endswith(".csv"):
                    matching_objects.append(object)
            else:
                matching_objects.append(object)
        return matching_objects

    def get_matching_objects(self, path, only_csv=True):
        matching_objects = []
        all_objects = self.bucket.objects.all()
        for object in all_objects:
            o_name = object.key
            if o_name.startswith(path):
                if only_csv:
                    if o_name.endswith(".csv"):
                        matching_objects.append(object)
                else:
                    matching_objects.append(object)
        return matching_objects

    def get_all_object_keys(self, only_csv=True):
        all_objects = self.bucket.objects.all()
        matching_objects = []
        for object in all_objects:
            o_name = object.key
            if only_csv:
                if o_name.endswith(".csv"):
                    matching_objects.append(o_name)
            else:
                matching_objects.append(o_name)
        return matching_objects

    def get_matching_object_keys(self, path, only_csv=True):
        matching_objects = []
        all_objects = self.bucket.objects.all()
        for object in all_objects:
            o_name = object.key
            if o_name.startswith(path):
                if only_csv:
                    if o_name.endswith(".csv"):
                        matching_objects.append(o_name)
                else:
                    matching_objects.append(o_name)
        return matching_objects

    def delete_object(self, object_key):
        if not self.validate_key_exisits(object_key):
            raise Exception("Key {k} does not exisit in bucket.".format(k=object_key))
        self.s3.Object(self.bucket_name, object_key).delete()

    def delete_objects(self, object_keys):
        if not self.validate_keys_exisit(object_keys):
            raise Exception("Not all Keys exisit in bucket. Nothing was deleted.")
        for object_key in object_keys:
            self.s3.Object(self.bucket_name, object_key).delete()

    def validate_key_exisits(self, key):
        object_keys = self.get_all_object_keys(only_csv=False)
        if key not in object_keys:
            return False
        return True

    def validate_keys_exisit(self, keys):
        object_keys = self.get_all_object_keys(only_csv=False)
        flag = True

        for key in keys:
            if key not in object_keys:
                flag = False

        return flag

    def put_pandas_as_csv(self, df, key):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        response = self.s3.Object(self.bucket_name, key).put(Body=csv_buffer.getvalue())
        return response['ResponseMetadata']['HTTPStatusCode']

    def download_object(self, key, force=False):
        if not isinstance(key, str):
            raise Exception("Key {k} is not a string.".format(k=key))
        if not self.validate_key_exisits(key):
            raise Exception("Key {k} does not exisit in bucket.".format(k=key))

        translated_key = key.replace("/", "\\")
        contents = listdir(self.data_dir)
        if translated_key in contents and not force:
            return 0
        loc = self.data_dir + translated_key
        self.bucket.download_file(key, loc)
        return 1

    def download_objects(self, keys, force_keys=[]):
        count = 0
        for key in keys:
            if key in force_keys:
                count += self.download_object(key, force=True)
            else:
                count += self.download_object(key)
        return count

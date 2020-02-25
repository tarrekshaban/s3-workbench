import boto3
from os import path


class Workbench:
    def __init__(self, bucket, data_dir=None):
        self.data_dir = data_dir
        self.s3 = boto3.resource('s3', use_ssl=False)
        self.bucket = self.s3.Bucket(bucket)

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


if __name__ == "__main__":
    data_dir = "/home/tarrek/Projects/twitterdata/"
    bucket = "primary-tweets-2020"
    twitter = Workbench(bucket)
    print(twitter.set_data_directory(data_dir))

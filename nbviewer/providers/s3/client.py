from typing import List
from io import BytesIO
import os

import boto3
import botocore


class S3Client:
    def __init__(self):
        self._cli = boto3.client("s3")
        self._s3_bucket = os.environ.get('S3_BUCKET')
        self._s3_prfix = os.environ.get('S3_PREFIX', '')

    def _namespaced_path(self, path):
        if path == '':
            path = '/'
        if path[0] != "/":
            path = "/" + path
        path = self._s3_prfix + path
        if path[0] == '/':
            path = path[1:]
        return path

    def _unnamespaced_path(self, path):
        if len(self._s3_prfix) > 0:
            return path[len(self._s3_prfix) + 1 :]
        return path

    def list(self, prefix) -> List[str]:

        namespaced = self._namespaced_path(prefix)
        paths = []

        next_token = ""

        while True:
            if next_token == "":
                response = self._cli.list_objects_v2(
                    Bucket=self._s3_bucket, Prefix=namespaced, Delimiter='/'
                )
            else:
                response = self._cli.list_objects_v2(
                    Bucket=self._s3_bucket,
                    Prefix=namespaced,
                    ContinuationToken=next_token,
                    Delimiter='/'
                )

            for content in response.get('CommonPrefixes', []):

                paths.append(dict(
                    path=self._unnamespaced_path(content["Prefix"]),
                    name=content["Prefix"][len(namespaced):-1],
                    type='dir',
                ))

            for content in response.get('Contents', []):

                paths.append(dict(
                    path=self._unnamespaced_path(content["Key"]),
                    name=content["Key"][len(namespaced):],
                    type='file',
                    last_modified=content['LastModified'],
                ))

            if "NextContinuationToken" in response:
                next_token = response["NextContinuationToken"]
            else:
                break

        return paths

    def get_object(self, path):
        key = self._namespaced_path(path)
        resp = self._cli.get_object(Bucket=self._s3_bucket, Key=key)
        return dict(
            body=resp['Body'],
            content_length=resp['ContentLength'],
        )

    def is_dir(self, path):
        key = self._namespaced_path(path)

        if len(key) == 0 or key[-1] == '/':
            return True

        resp = self._cli.list_objects_v2(Bucket=self._s3_bucket, Prefix=key, Delimiter='/')

        prefixes = resp.get('CommonPrefixes', [])

        if len(prefixes) > 0:

            if prefixes[0]['Prefix'] == key + '/':
                return True

        return False

    def exist_object(self, path):
        try:
            self._cli.head_object(
                Bucket=self._s3_bucket, Key=self._namespaced_path(path)
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise e
        return True

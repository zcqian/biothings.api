"""
    Biothings Test Helper

    Envs:

    TEST_SCHEME
    TEST_PREFIX
    TEST_HOST
    TEST_CONF

"""
import glob
import json
import os
from functools import partial

import elasticsearch
import pytest
import requests
from tornado.ioloop import IOLoop
from tornado.testing import AsyncHTTPTestCase

from biothings.web.settings import BiothingESWebSettings


class BiothingsDataTest():

    # relative path parsing configuration
    scheme = 'http'
    prefix = 'v1'
    host = ''

    def request(self, path, method='GET', expect=200, **kwargs):
        """
        Use requests library to make an HTTP request.
        Ensure path is translated to an absolute path.
        Conveniently check if status code is as expected.
        """
        url = self.get_url(path)
        res = requests.request(method, url, **kwargs)
        assert res.status_code == expect, res.text

        return res

    def get_url(self, path):
        """
        Try best effort to get a full url to make a request.
        Return an absolute url when class var 'host' is defined.
        If not, return a path relative to the host root.
        """
        scheme = os.getenv("TEST_SCHEME", self.scheme)
        prefix = os.getenv("TEST_PREFIX", self.prefix).strip('/')
        host = os.getenv("TEST_HOST", self.host).strip('/')

        # already an absolute path
        if path.lower().startswith(("http://", "https://")):
            return path

        # path standardization
        if not path.startswith('/'):
            if prefix:  # append prefix
                path = '/'.join((prefix, path))
            path = '/' + path

        # host standardization
        if host:
            path = f"{scheme}://{host}{path}"

        return path

    def query(self, method='GET', endpoint='query', hits=True, data=None, json=None, **kwargs):
        """
        Make a query and assert positive hits by default.
        Assert zero hit when hits is set to False.
        """

        if method == 'GET':
            res = self.request(
                endpoint, params=kwargs,
                data=data, json=json).json()

            assert bool(res.get('hits')) == hits
            return res

        if method == 'POST':
            res = self.request(
                endpoint, method=method,
                params=kwargs, data=data,
                json=json).json()

            for item in res:  # list
                if "_id" not in item:
                    _hits = False
                    break
            else:
                _hits = bool(res)
            assert _hits is hits
            return res

        raise ValueError('Invalid Request Method.')

    @staticmethod
    def msgpack_ok(packed_bytes):
        """ Load msgpack into a dict """
        try:
            import msgpack
        except ImportError:
            pytest.skip('Msgpack is not installed.')
        try:
            dic = msgpack.unpackb(packed_bytes)
        except BaseException:  # pylint: disable=bare-except
            assert False, 'Not a valid Msgpack binary.'
        return dic


class BiothingsWebAppTest(BiothingsDataTest, AsyncHTTPTestCase):
    """
        Starts the tornado application to run tests locally.
        Need a config.py under the current working dir.
    """

    def _process_es_data_dir(self, data_dir_path):
        client = elasticsearch.Elasticsearch(self.settings.ES_HOST)
        server_major_version = client.info()['version']['number'].split('.')[0]
        client_major_version = str(elasticsearch.__version__[0])
        if server_major_version != client_major_version:
            pytest.exit('ES version does not match its python library.')

        # FIXME: this is broken
        default_doc_type = self.settings.ES_DOC_TYPE

        indices = []
        glob_json_pattern = os.path.join(data_dir_path, '*.json')
        # FIXME: wrap around in try-finally so the index is guaranteed to be
        #  cleaned up
        for index_mapping_path in glob.glob(glob_json_pattern):
            index_name = os.path.basename(index_mapping_path)
            index_name = os.path.splitext(index_name)[0]
            indices.append(index_name)
            if client.indices.exists(index_name):
                raise Exception(f"{index_name} already exists!")
            with open(index_mapping_path, 'r') as f:
                mapping = json.load(f)
            data_path = os.path.join(data_dir_path, index_name + '.ndjson')
            with open(data_path, 'r') as f:
                bulk_data = f.read()
            if elasticsearch.__version__[0] > 6:
                client.indices.create(index_name, mapping, include_type_name=True)
                client.bulk(bulk_data, index_name)
            else:
                client.indices.create(index_name, mapping)
                # FIXME: doc_type problem in ES6
                # client.bulk(bulk_data, index_name, default_doc_type)
                client.bulk(bulk_data, index_name, "_doc")

            client.indices.refresh()
        yield
        for index_name in indices:
            client.indices.delete(index_name)

    @pytest.fixture(scope="class", autouse=True)
    def load_es_data_cls(self, request):
        data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'test_data',
            getattr(self, 'TEST_DATA_NAME', request.cls.__name__)
        )
        if not os.path.exists(data_dir):
            if hasattr(self, 'TEST_DATA_NAME'):
                pytest.exit(f"TEST_DATA_NAME set but {data_dir} not present")
            else:  # not explicitly set, assume no data
                pass
        else:
            yield from self._process_es_data_dir(data_dir)

    @classmethod
    def setup_class(cls):
        conf = os.getenv("TEST_CONF", 'config')
        cls.settings = BiothingESWebSettings(conf)
        prefix = cls.settings.API_PREFIX
        version = cls.settings.API_VERSION
        cls.prefix = f'{prefix}/{version}'

    # override
    def get_new_ioloop(self):
        return IOLoop.current()

    # override
    def get_app(self):
        return self.settings.get_app()

    # override
    def request(self, path, method="GET", expect=200, **kwargs):

        url = self.get_url(path)

        func = partial(requests.request, method, url, **kwargs)
        res = self.io_loop.run_sync(
            lambda: self.io_loop.run_in_executor(None, func))

        assert res.status_code == expect, res.text
        return res

    # override
    def get_url(self, path):

        path = BiothingsDataTest.get_url(self, path)
        return AsyncHTTPTestCase.get_url(self, path)


# Compatibility
BiothingsTestCase = BiothingsWebAppTest

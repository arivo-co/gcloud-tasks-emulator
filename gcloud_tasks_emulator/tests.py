import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import TestCase as BaseTestCase

import grpc
import sleuth
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import Unknown
from google.cloud.tasks_v2 import CloudTasksClient
from google.cloud.tasks_v2.gapic.transports.cloud_tasks_grpc_transport import \
    CloudTasksGrpcTransport

from server import create_server


class MockRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.end_headers()


class MockServer(threading.Thread):
    def __init__(self, port, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._port = port
        self.is_running = threading.Event()
        self._httpd = None

    def run(self):
        self._httpd = HTTPServer(("localhost", self._port), MockRequestHandler)
        self.is_running.set()
        self._httpd.serve_forever()

    def join(self, timeout=None):
        self.is_running.clear()
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()


class TestCase(BaseTestCase):
    def setUp(self):
        self._server = create_server("localhost", 9022)
        self._server.start()
        time.sleep(1)

        transport = CloudTasksGrpcTransport(channel=grpc.insecure_channel("127.0.0.1:9022"))

        self._client = CloudTasksClient(
            transport=transport,
            client_options=ClientOptions(api_endpoint="127.0.0.1:9022")
        )

        self._parent = self._client.location_path('[PROJECT]', '[LOCATION]')

    def tearDown(self):
        self._server.stop()

    def test_create_queue(self):
        ret = self._client.create_queue(self._parent, {"name": "test_queue1"})
        self.assertEqual(ret.name, "test_queue1")

        ret = self._client.create_queue(self._parent, {"name": "test_queue2"})
        self.assertEqual(ret.name, "test_queue2")

    def test_list_queues(self):
        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "default")
        self._client.delete_queue(path)

        self.test_create_queue()  # Create a couple of queues

        queues = self._client.list_queues(parent=self._parent)
        self.assertEqual(len(list(queues)), 2)

    def test_get_queue(self):
        self.test_create_queue()  # Create a couple of queues

        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "test_queue2")
        queue = self._client.get_queue(path)
        self.assertEqual(queue.name, "test_queue2")

    def test_delete_queue(self):
        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "default")
        self._client.delete_queue(path)

        self.test_create_queue()  # Create a couple of queues

        queues = self._client.list_queues(parent=self._parent)
        self.assertEqual(len(list(queues)), 2)

        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "test_queue2")
        self._client.delete_queue(path)

        queues = self._client.list_queues(parent=self._parent)
        self.assertEqual(len(list(queues)), 1)

    def test_pause_queue(self):
        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "default")
        self._client.delete_queue(path)

        self.test_create_queue()  # Create a couple of queues

        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "test_queue2")
        response = self._client.pause_queue(path)
        self.assertEqual(response.state, 2)

    def test_purge_queue(self):
        self.test_create_queue()  # Create a couple of queues

        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "test_queue2")

        # Pause the queue as we don't want tasks to be processed
        self._client.pause_queue(path)

        payload = "Hello World!"

        task = {
            'app_engine_http_request': {  # Specify the type of request.
                'http_method': 'POST',
                'relative_uri': '/example_task_handler',
                'body': payload.encode()
            }
        }

        # Create 3 tasks
        self._client.create_task(path, task)
        self._client.create_task(path, task)
        self._client.create_task(path, task)

        tasks = [x for x in self._client.list_tasks(path)]
        self.assertEqual(len(tasks), 3)

        # Check again, make sure list_tasks didn't do anything
        tasks = [x for x in self._client.list_tasks(path)]
        self.assertEqual(len(tasks), 3)

        self._client.purge_queue(path)

        tasks = [x for x in self._client.list_tasks(path)]
        self.assertEqual(len(tasks), 0)

    def test_create_task(self):
        self.test_create_queue()  # Create a couple of queues

        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "test_queue2")

        self._client.pause_queue(path)
        payload = "Hello World!"

        task = {
            'app_engine_http_request': {  # Specify the type of request.
                'http_method': 'POST',
                'relative_uri': '/example_task_handler',
                'body': payload.encode()
            }
        }

        response = self._client.create_task(path, task)
        self.assertTrue(response.name.startswith(path))

    def test_run_task(self):
        self.test_create_queue()  # Create a couple of queues

        self._client.pause_queue("test_queue2")  # Don't run any tasks while testing

        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "test_queue2")
        payload = "Hello World!"

        task = {
            'app_engine_http_request': {  # Specify the type of request.
                'http_method': 'POST',
                'relative_uri': '/example_task_handler',
                'body': payload.encode()
            }
        }

        response = self._client.create_task(path, task)
        self.assertTrue(response.name.startswith(path))

        class FakeResponse:
            status = 200

        with sleuth.fake("server._make_task_request", return_value=FakeResponse()):
            self._client.run_task(response.name)

        # Should return NOT_FOUND
        self.assertRaises(
            Unknown,
            self._client.run_task,
            "%s/tasks/1119129292929292929" % path,  # Not a valid task
        )


class CustomPortTestCase(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        cls._server = MockServer(10123)
        cls._server.start()
        cls._server.is_running.wait()

    @classmethod
    def tearDownClass(cls):
        cls._server.join(timeout=1)

    def setUp(self):
        self._server = create_server("localhost", 9022, 10123)
        self._server.start()
        time.sleep(1)

        transport = CloudTasksGrpcTransport(channel=grpc.insecure_channel("127.0.0.1:9022"))

        self._client = CloudTasksClient(
            transport=transport,
            client_options=ClientOptions(api_endpoint="127.0.0.1:9022")
        )

        self._parent = self._client.location_path('[PROJECT]', '[LOCATION]')

    def tearDown(self):
        self._server.stop()

    def test_create_queue(self):
        ret = self._client.create_queue(self._parent, {"name": "test_queue1"})
        self.assertEqual(ret.name, "test_queue1")

        ret = self._client.create_queue(self._parent, {"name": "test_queue2"})
        self.assertEqual(ret.name, "test_queue2")

    def test_run_task(self):
        self.test_create_queue()  # Create a couple of queues

        self._client.pause_queue("test_queue2")  # Don't run any tasks while testing

        path = self._client.queue_path('[PROJECT]', '[LOCATION]', "test_queue2")
        payload = "Hello World!"

        task = {
            'app_engine_http_request': {  # Specify the type of request.
                'http_method': 'POST',
                'relative_uri': '/example_task_handler',
                'body': payload.encode()
            }
        }

        response = self._client.create_task(path, task)
        self.assertTrue(response.name.startswith(path))

        self._client.run_task(response.name)


if __name__ == '__main__':
    unittest.main()

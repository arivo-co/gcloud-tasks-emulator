import unittest
from unittest import TestCase as BaseTestCase

from server import create_server

from google.cloud.tasks_v2 import CloudTasksClient
from google.cloud.tasks_v2.gapic.transports.cloud_tasks_grpc_transport import CloudTasksGrpcTransport

from google.api_core.client_options import ClientOptions

import grpc
import time


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


if __name__ == '__main__':
    unittest.main()

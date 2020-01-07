
from datetime import datetime
import logging
import sys
import time
import threading
import grpc
from concurrent import futures

from google.protobuf import empty_pb2
from google.cloud.tasks_v2.proto import cloudtasks_pb2
from google.cloud.tasks_v2.proto import cloudtasks_pb2_grpc
from google.cloud.tasks_v2.proto import queue_pb2
from google.cloud.tasks_v2.proto import task_pb2


Queue = queue_pb2.Queue
Task = task_pb2.Task


class QueueState(object):
    """
        Keeps the state of queues and tasks in memory
        so they can be processed
    """

    def __init__(self):
        self._queues = {}
        self._queue_tasks = {}

    def create_queue(self, name, **options):
        if name not in self._queues:
            self._queues[name] = Queue(name=name)
            self._queue_tasks[name] = []
            return self._queues[name]

    def create_task(self, queue, task):
        queue_name = queue.rsplit("/", 1)[-1]
        task_name = task.name or "%s/tasks/%s" % (
            queue, int(datetime.now().timestamp())
        )

        new_task = Task(name=task_name)
        self._queue_tasks[queue_name].append(new_task)
        return new_task

    def queue_names(self):
        return list(self._queues)

    def queues(self):
        return list(self._queues.values())

    def queue(self, name):
        return self._queues[name]

    def delete_queue(self, name):
        del self._queues[name]


class Greeter(cloudtasks_pb2_grpc.CloudTasksServicer):
    def __init__(self, state):
        super().__init__()

        self._state = state

    def CreateQueue(self, request, context):
        return self._state.create_queue(request.queue.name)

    def ListQueues(self, request, context):
        return cloudtasks_pb2.ListQueuesResponse(queues=self._state.queues())

    def GetQueue(self, request, context):
        queue_name = request.name.rsplit("/", 1)[-1]
        return self._state.queue(queue_name)

    def DeleteQueue(self, request, context):
        queue_name = request.name.rsplit("/", 1)[-1]
        self._state.delete_queue(queue_name)
        return empty_pb2.Empty()

    def CreateTask(self, request, context):
        return self._state.create_task(request.parent, request.task)


class APIThread(threading.Thread):
    def __init__(self, state, host, port, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._state = state
        self._port = port
        self._is_running = threading.Event()
        self._httpd = None

    def run(self):
        self._httpd = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        cloudtasks_pb2_grpc.add_CloudTasksServicer_to_server(Greeter(self._state), self._httpd)

        interface = '[::]:%s' % self._port
        self._httpd.add_insecure_port(interface)

        logging.info("[API] Starting API server at %s", interface)
        self._httpd.start()

        while self._is_running.is_set():
            time.sleep(0)

    def join(self, timeout=None):
        self._is_running.clear()
        if self._httpd:
            self._httpd.stop(grace=0)
        logging.info("[API] Stopping API server")


class Processor(threading.Thread):
    def __init__(self, state):
        super().__init__()

        self._state = state
        self._is_running = threading.Event()

    def run(self):
        self._is_running.set()

        logging.info("[API] Starting task processor")
        while self._is_running.is_set():
            queue_names = self._state.queue_names()
            for queue in queue_names:
                self.process_queue(queue)
                time.sleep(0)

    def process_queue(self, queue_name):
        pass

    def join(self, timeout=None):
        self._is_running.clear()
        super().join(timeout)


class Server(object):
    def __init__(self, host, port):
        self._state = QueueState()
        self._api = APIThread(self._state, host, port)
        self._processor = Processor(self._state)

    def start(self):
        self._api.start()  # Start the API thread
        self._processor.start()

    def stop(self):
        self._processor.join(timeout=1)
        self._api.join(timeout=1)

    def run(self):
        try:
            self.start()

            logging.info("[SERVER] All services started")

            while True:
                try:
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    break

        finally:
            self.stop()


def create_server(host, port):
    return Server(host, port)

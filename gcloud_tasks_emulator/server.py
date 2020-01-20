
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
from google.api_core.exceptions import NotFound


Queue = queue_pb2.Queue
Task = task_pb2.Task


logger = logging.getLogger("gcloud-tasks-emulator")


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
            self._queues[name] = Queue(
                name=name,
                state=queue_pb2._QUEUE_STATE.values_by_name["RUNNING"].number
            )
            self._queue_tasks[name] = []
            logger.info("[TASKS] Created queue %s", name)

        return self._queues[name]

    def create_task(self, queue, task):
        queue_name = queue.rsplit("/", 1)[-1]
        task_name = task.name or "%s/tasks/%s" % (
            queue, int(datetime.now().timestamp())
        )

        new_task = Task(name=task_name)
        self._queue_tasks[queue_name].append(new_task)
        logger.info("[TASKS] Created task %s", task_name)
        return new_task

    def purge_queue(self, queue):
        queue_name = queue.rsplit("/", 1)[-1]
        if queue_name in self._queues:
            # Wipe the tasks out
            logger.info("[TASKS] Purging queue %s", queue_name)
            self._queue_tasks[queue_name] = []
            return self._queues[queue_name]
        else:
            raise ValueError()

    def pause_queue(self, queue):
        queue_name = queue.rsplit("/", 1)[-1]
        if queue_name in self._queues:
            logger.info("[TASKS] Pausing queue %s", queue_name)
            self._queues[queue_name].state = queue_pb2._QUEUE_STATE.values_by_name["PAUSED"].number
            return self._queues[queue_name]
        else:
            raise ValueError()

    def list_tasks(self, queue):
        queue_name = queue.rsplit("/", 1)[-1]
        return self._queue_tasks[queue_name]

    def queue_names(self):
        return list(self._queues)

    def queues(self):
        return list(self._queues.values())

    def queue(self, name):
        return self._queues[name]

    def delete_queue(self, name):
        if name in self._queues:
            logger.info("[TASKS] Deleting queue %s", name)
            del self._queues[name]

        if name in self._queue_tasks:
            del self._queue_tasks[name]

    def submit_task(self, task_name):
        def make_task_request(task):
            logger.info("[TASKS] Submitting task %s", task_name)
            pass

        index = None
        try:
            queue_name = task_name.rsplit("/")[-3]
            if queue_name not in self._queue_tasks:
                raise ValueError("Not a valid queue")
        except IndexError:
            # Invalid task name, raise ValueError
            raise ValueError()

        for i, task in enumerate(self._queue_tasks[queue_name]):
            if task.name == task_name:
                index = i
                break
        else:
            raise NotFound("Task not found: %s" % task_name)

        task = self._queue_tasks[queue_name].pop(index)  # Remove the task we found
        make_task_request(task)

        # FIXME: Do submission and requeue the task if it fails
        # FIXME: Set task.first_attempt / last_attempt and other metadata

        assert(task)
        return task


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

    def PauseQueue(self, request, context):
        return self._state.pause_queue(request.name)

    def PurgeQueue(self, request, context):
        return self._state.purge_queue(request.name)

    def ListTasks(self, request, context):
        return cloudtasks_pb2.ListTasksResponse(
            tasks=self._state.list_tasks(request.parent)
        )

    def DeleteQueue(self, request, context):
        queue_name = request.name.rsplit("/", 1)[-1]
        self._state.delete_queue(queue_name)
        return empty_pb2.Empty()

    def CreateTask(self, request, context):
        return self._state.create_task(request.parent, request.task)

    def RunTask(self, request, context):
        return self._state.submit_task(request.name)


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

        logger.info("[API] Starting API server at %s", interface)
        self._httpd.start()

        while self._is_running.is_set():
            time.sleep(0)

    def join(self, timeout=None):
        self._is_running.clear()
        if self._httpd:
            self._httpd.stop(grace=0)
        logger.info("[API] Stopping API server")


class Processor(threading.Thread):
    def __init__(self, state):
        super().__init__()

        self._state = state
        self._is_running = threading.Event()
        self._known_queues = set()
        self._queue_threads = {}

    def run(self):
        self._is_running.set()

        logger.info("[PROCESSOR] Starting task processor")
        while self._is_running.is_set():
            queue_names = self._state.queue_names()
            for queue in queue_names:
                self.process_queue(queue)
                time.sleep(0)

    def _submit_task(self, task):
        logging.debug("[PROCESSOR] submitting task %s", task)
        pass

    def _process_queue(self, queue):
        while self._is_running.is_set():
            # Queue was deleted, stop processing
            if queue not in self._state._queues:
                break

            if queue not in self._state._queue_tasks:
                break

            if self._state.queue(queue).state == queue_pb2._QUEUE_STATE.values_by_name["RUNNING"].number:
                tasks = self._state._queue_tasks[queue][:]
                while tasks:
                    task = tasks.pop(0)
                    logging.info("[TASKS] Processing next task %s", task.name)
                    self._state.submit_task(task.name)

            time.sleep(0)

    def process_queue(self, queue_name):
        if queue_name not in self._known_queues:
            # A queue was just created
            self._known_queues.add(queue_name)

            thread = threading.Thread(
                target=self._process_queue, args=[queue_name]
            )
            self._queue_threads[queue_name] = thread
            self._queue_threads[queue_name].start()

    def join(self, timeout=None):
        self._is_running.clear()
        for thread in self._queue_threads.values():
            if thread.is_alive():
                thread.join(timeout=0)

        super().join(timeout)


class Server(object):
    def __init__(self, host, port):
        self._state = QueueState()

        # Always start with a default queue (like App Engine)
        self._state.create_queue("default")

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

            logger.info("[SERVER] All services started")

            while True:
                try:
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    break

        finally:
            self.stop()


def create_server(host, port):
    return Server(host, port)

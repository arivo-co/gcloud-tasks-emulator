
import logging
import threading
import time
from concurrent import futures
from datetime import datetime
from urllib import error, request
from urllib.parse import parse_qs

import grpc
from google.api_core.exceptions import NotFound
from google.cloud.tasks_v2.proto import (cloudtasks_pb2, cloudtasks_pb2_grpc,
                                         queue_pb2, task_pb2)
from google.protobuf import empty_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from google.rpc.status_pb2 import Status

# Time to sleep between iterations of the threads
_LOOP_SLEEP_TIME = 0.1

DEFAULT_TARGET_PORT = 80

Queue = queue_pb2.Queue
Task = task_pb2.Task
Attempt = task_pb2.Attempt


logger = logging.getLogger("gcloud-tasks-emulator")


def _make_task_request(queue_name, task, port):
    logger.info("[TASKS] Submitting task %s", task.name)

    headers = {}
    data = None

    if task.app_engine_http_request:
        data = task.app_engine_http_request.body

        url = "http://localhost:%s%s" % (
            port,
            task.app_engine_http_request.relative_uri
        )

        headers.update({
            'X-AppEngine-QueueName': queue_name,
            'X-AppEngine-TaskName': task.name.rsplit("/", 1)[-1],
            'X-AppEngine-TaskRetryCount': 0,  # FIXME: Populate
            'X-AppEngine-TaskExecutionCount': 0,  # FIXME: Populate
            'X-AppEngine-TaskETA': 0,  # FIXME: Populate
        })
    else:
        assert(task.http_request)  # FIXME:
        pass

    req = request.Request(url, data=data)
    for k, v in headers.items():
        req.add_header(k, v)

    return request.urlopen(req)


class QueueState(object):
    """
        Keeps the state of queues and tasks in memory
        so they can be processed
    """

    def __init__(self, target_port):
        self._queues = {}
        self._queue_tasks = {}
        self._target_port = target_port

    def create_queue(self, parent, queue):
        assert(queue.name)

        if queue.name not in self._queues:
            self._queues[queue.name] = queue
            self._queues[queue.name].state = (
                queue_pb2._QUEUE_STATE.values_by_name["RUNNING"].number
            )
            self._queue_tasks[queue.name] = []
            logger.info("[TASKS] Created queue %s", queue.name)

        return self._queues[queue.name]

    def create_task(self, queue, task):
        queue_name = queue.rsplit("/", 1)[-1]
        task.name = task.name or "%s/tasks/%s" % (
            queue, int(datetime.now().timestamp())
        )

        if task.app_engine_http_request:
            # Set a default http_method
            task.app_engine_http_request.http_method = (
                task.app_engine_http_request.http_method or "POST"
            )

        self._queue_tasks[queue_name].append(task)
        logger.info("[TASKS] Created task %s", task.name)
        return task

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
            self._queues[queue_name].state = (
                queue_pb2._QUEUE_STATE.values_by_name["PAUSED"].number
            )
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
        try:
            queue_name = task_name.rsplit("/")[-3]
            if queue_name not in self._queue_tasks:
                raise ValueError("Not a valid queue")
        except IndexError:
            # Invalid task name, raise ValueError
            raise ValueError()

        # This is a special-case that does not exist
        # one the live server and it exists so that
        # local development servers can direct a task
        # to run on a particular port
        qs = task_name.rsplit("?", 1)[-1]
        if qs:
            params = parse_qs(qs)
            port = int(params.get("port", [self._target_port])[0])
            task_name = task_name.rsplit("?", 1)[0]
        else:
            port = self._target_port

        index = None

        # Locate the task in the queue
        for i, task in enumerate(self._queue_tasks[queue_name]):
            if task.name == task_name:
                index = i
                break
        else:
            logger.debug(
                "[TASKS] Tasks were: %s",
                [x.name for x in self._queue_tasks[queue_name]]
            )
            raise NotFound("Task not found: %s" % task_name)

        def now():
            return Timestamp(seconds=int(datetime.now().timestamp()))

        schedule_time = now()
        dispatch_time = None
        response_time = None
        response_status = 200

        task = self._queue_tasks[queue_name].pop(index)  # Remove the task
        try:
            dispatch_time = now()
            response = _make_task_request(queue_name, task, port)
        except error.HTTPError as e:
            response_status = e.code
            logging.error("Error submitting task, moving to the back of the queue")
            logging.error("Reason was: %s" % e.reason)
            self._queue_tasks[queue_name].append(task)
        except (ConnectionRefusedError, error.URLError):
            response_status = 500
            logger.exception(
                "Error submitting task, moving to the back of the queue"
            )
            self._queue_tasks[queue_name].append(task)
        else:
            response_status = response.status

        attempt = Attempt(
            schedule_time=schedule_time,
            dispatch_time=dispatch_time,
            response_time=response_time,
            response_status=Status(code=response_status)
        )

        kwargs = {
            "first_attempt": task.first_attempt or attempt,
            "last_attempt": attempt
        }

        task.MergeFrom(Task(**kwargs))

        assert(task)
        return task


class Greeter(cloudtasks_pb2_grpc.CloudTasksServicer):
    def __init__(self, state):
        super().__init__()

        self._state = state

    def CreateQueue(self, request, context):
        return self._state.create_queue(request.parent, request.queue)

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
        self._httpd = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        cloudtasks_pb2_grpc.add_CloudTasksServicer_to_server(
            Greeter(self._state), self._httpd
        )

        interface = '[::]:%s' % self._port
        self._httpd.add_insecure_port(interface)

        logger.info("[API] Starting API server at %s", interface)
        self._httpd.start()

        while self._is_running.is_set():
            time.sleep(_LOOP_SLEEP_TIME)

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
            time.sleep(_LOOP_SLEEP_TIME)

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
                    logger.info("[TASKS] Processing next task %s", task.name)
                    self._state.submit_task(task.name)

            time.sleep(_LOOP_SLEEP_TIME)

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
    def __init__(self, host, port, target_port):
        self._state = QueueState(target_port)

        # Always start with a default queue (like App Engine)
        self._state.create_queue("", Queue(name="default"))

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
                    time.sleep(_LOOP_SLEEP_TIME)
                except KeyboardInterrupt:
                    break

        finally:
            self.stop()


def create_server(host, port, target_port=DEFAULT_TARGET_PORT):
    return Server(host, port, target_port)

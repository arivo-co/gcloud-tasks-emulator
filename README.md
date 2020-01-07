# Local Emulator for Google Cloud Tasks

Google doesn't (yet) ship an emulator for the Cloud Tasks API like they do for
Cloud Datastore.

This is a stub emulator so you can run your tests and do local development without
having to connect to the production Tasks API.

**THIS IS A WORK IN PROGRESS NOT ALL API CALLS ARE COMPLETE**

## Usage

Start the emulator with:

```
gcloud-tasks-emulator start --port=9090
```

Then from within your code, use the following (instead of your normal production client connection)

```
from google.cloud.tasks_v2 import CloudTasksClient
from google.cloud.tasks_v2.gapic.transports.cloud_tasks_grpc_transport import CloudTasksGrpcTransport
from google.api_core.client_options import ClientOptions

client = CloudTasksClient(
    transport=CloudTasksGrpcTransport(channel=grpc.insecure_channel("127.0.0.1:%s" % port)),
    client_options=ClientOptions(api_endpoint="127.0.0.1:%s" % port)
)
```

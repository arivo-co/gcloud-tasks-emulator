FROM gcr.io/google-appengine/python

RUN virtualenv -p python3.7 /env

# Setting these environment variables are the same as running
# source /env/bin/activate.
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

WORKDIR /app

# Copy the application's requirements.txt and run pip to install all
# dependencies into the virtualenv.
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Add the application source code.
ADD gcloud_tasks_emulator/ /app/gcloud_tasks_emulator/
ADD bin/ /app/

EXPOSE 80

# Run a WSGI server to serve the application.
CMD [ "python", "gcloud-tasks-emulator", "start", "--port=80" ]

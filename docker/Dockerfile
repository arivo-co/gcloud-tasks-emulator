FROM python:3.8-slim
WORKDIR /app


# Add the application source code.
COPY . /app
RUN pip install -r /app/requirements.txt
ENV PYTHONPATH=/app

EXPOSE 80

# Run a WSGI server to serve the application.
CMD [ "python", "bin/gcloud-tasks-emulator", "start", "--port=80" ]

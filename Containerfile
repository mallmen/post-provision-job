FROM registry.access.redhat.com/ubi9/python-311:latest
WORKDIR /app
RUN pip install --no-cache-dir kubernetes
COPY watcher.py /app/watcher.py
USER 1001
CMD ["python", "-u", "/app/watcher.py"]

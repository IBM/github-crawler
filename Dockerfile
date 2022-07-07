FROM registry.access.redhat.com/ubi9/python-39

COPY requirements.txt .
COPY *.py .
COPY .env .
COPY utils utils

RUN pip install -r requirements.txt

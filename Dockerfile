# # Use the official Python image as the base image and OpenJDK for spark dependencies

ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.8

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

RUN mkdir /abnamro

# Copy your Python application code into the container

COPY . /abnamro/

WORKDIR /abnamro

RUN mkdir /abnamro/client_data

RUN mkdir /abnamro/logs

RUN pip install -r requirements.txt

CMD ["python", "main.py", "dataset_one.csv" ,"dataset_two.csv" ,"Netherlands" ,"United Kingdom"]

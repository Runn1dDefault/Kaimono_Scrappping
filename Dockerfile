FROM python:3.11

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update

WORKDIR /app

RUN pip install --upgrade pip

COPY . .

RUN pip install -r requirements.txt

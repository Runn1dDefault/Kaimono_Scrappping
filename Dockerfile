FROM python:3.11

RUN apt-get update

WORKDIR /app

RUN pip install --upgrade pip

COPY . .

RUN pip install -r requirements.txt

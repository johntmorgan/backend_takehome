# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

RUN apt-get update -y && apt-get install -y build-essential

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "-m" , "flask", "run", "--host=0.0.0.0"]

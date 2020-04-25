FROM ubuntu:latest

RUN apt update && apt install -y python3 python3-pip

RUN pip3 install docker flask pika APScheduler==3.6.3

CMD ["python3", "orchestrator.py"]

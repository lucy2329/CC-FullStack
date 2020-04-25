
#!/usr/bin/python
# -*- coding: utf-8 -*-
import docker
import flask
import json
import os
import pika
from time import sleep
from flask import jsonify
# import uuid
import math
print("latest")

from apscheduler.schedulers.background import BackgroundScheduler




class ReaderClient(object):

    def __init__(self, connect):
        self.connect = connect
        self.channel = connect.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, body):
        self.response = None
        self.corr_id = str(100)
        self.channel.basic_publish(
            exchange='',
            routing_key='readQ',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=body)
        while self.response is None:
            self.connect.process_data_events()
        return self.response

class WriterClient(object):

    def __init__(self, connect):
        self.connect = connect
        self.channel = connect.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        # if self.corr_id == props.correlation_id:
        self.response = body
        print(self.response, "RESPONE", flush = True)

    def call(self, body, writerPID):
        self.response = None
        self.corr_id = str(100)
        self.channel.basic_publish(
            exchange='',
            routing_key='writeQ',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=body)
        while self.response is None:
            print("NONE", flush = True)
            self.connect.process_data_events()

        ret = str(self.response)
        print(ret)
        return self.response

sleep(15)

worker_status = []
worker_pids = []
master = ''

def get_pids():
    global worker_pids
    for worker in range(len(worker_status)):
        if worker_status[worker]==1:
            os.system('docker inspect --format \'{{ .State.Pid }}\' worker'+ str(worker) +' > tmp.txt')
            pid = ''
            with open('tmp.txt','r') as pid_file:
                for ch in pid_file:
                    pid += ch
            worker_pids[worker] = int(pid[:-1])
        else:
            worker_pids[worker] = -1

def elect_master():
    get_pids()
    global master
    master_pid = min([pid for pid in worker_pids if pid!=-1])
    master = str(worker_pids.index(master_pid))
    print('elected worker' + master, 'from', worker_pids, flush=True)

app = flask.Flask(__name__)



cron = BackgroundScheduler(daemon=True)

total_requests = 0
requests_per_2min = 0
index = 3
container_list = [-1]


client = docker.DockerClient(base_url='unix://var/run/docker.sock')

def job_function():
    global requests_per_2min
    global index
    global client
    global container_list
    print("hello i'm working ", flush = True)
    res = math.floor((requests_per_2min-1)/10)
    if(res<0):
        res = 0
    print("Requests ka value", res, flush = True)
    print("req/2min", requests_per_2min, flush = True)
    requests_per_2min = 0

    for i in range(res):
        print("creating new worker:",index, flush = True)
        w3_container = client.containers.run('worker:latest', name='worker'+str(index), environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=slave:'+str(index)],
                    network = 'Orchestrator', restart_policy={'Name': 'always'}, volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}, '/home/ubuntu/Orchestrator/filewrites.txt': {'bind': '/src/filewrites.txt', 'mode': 'ro'} },
                    detach=True)
        index += 1
        container_list.append(w3_container)
        worker_status.append(1)

job = cron.add_job(job_function, 'interval', minutes=1)
cron.start()

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', credentials,heartbeat=0)
connection = pika.BlockingConnection(parameters)

writerchannel = WriterClient(connection)
readerchannel = ReaderClient(connection)


w1_container = client.containers.run('worker:latest', name='worker1', environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=master:1'],
               network = 'Orchestrator', restart_policy={'Name': 'always'}, volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}},
               detach=True)
container_list.append(w1_container)
worker_status.append(1)
w2_container = client.containers.run('worker:latest', name='worker2', environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=slave:2'],
               network = 'Orchestrator', restart_policy={'Name': 'always'}, volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}},
               detach=True)
container_list.append(w2_container)
worker_status.append(1)


sleep(15)

get_pids()

@app.route("/api/v1/db/write", methods=["POST"])
def writeToDatabase():
    global total_requests
    global requests_per_2min
    body = flask.request.get_json()
    total_requests += 1
    requests_per_2min += 1
    print('worker'+master, flush=True)
    response = writerchannel.call(json.dumps(body), 'worker'+master)
    injson = json.loads(response.decode('utf-8'))
    print("RESPONSE", response, "INJSON", injson, flush = True)
    f = open("filewrites.txt", "a+")
    f.write(json.dumps(body)+"\n")
    f.close()
    return flask.Response("{}", status=200, mimetype="application/json")

@app.route("/api/v1/db/read", methods=["POST"])
def readFromDatabase():
    global total_requests
    global requests_per_2min
    total_requests += 1
    requests_per_2min += 1
    body = flask.request.get_json()
    response = readerchannel.call(json.dumps(body))
    print("RESPONSE", response, flush = True)
    injson = json.loads(response.decode('utf-8'))
    print("RESPONSE", response, "INJSON", injson, type(injson), flush = True)
    return jsonify(injson), 200

@app.route("/api/working", methods=["GET"])
def imWorking():
    print("IM WORKING", flush = True)
    return flask.Response("{}", status=200, mimetype="application/json")

if __name__=="__main__":
    app.debug = True
    app.run(host='0.0.0.0', use_reloader=False)

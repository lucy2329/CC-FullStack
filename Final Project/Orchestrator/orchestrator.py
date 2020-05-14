
#!/usr/bin/python
# -*- coding: utf-8 -*-
import docker
import subprocess
import flask
import json
import os
import pika
from time import sleep
from flask import jsonify
# import uuid
import math
print("latest")
from kazoo.client import KazooClient
sleep(10)
zk = KazooClient(hosts='zookeeper:2181')
zk.start()
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
global proc_id
proc_id=[]

worker_status = []
worker_pids = []
master = ''

def get_pids(number):
    global worker_pids
    #for worker in range(len(worker_status)):
        #if worker_status[worker]==1:
    os.system('docker inspect --format \'{{ .State.Pid }}\' worker'+ str(number) +' > tmp.txt')
    pid = ''
    with open('tmp.txt','r') as pid_file:
        for ch in pid_file:
            pid += ch
    return(int(pid[:-1]))

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
scaling_flag = 0


client = docker.DockerClient(base_url='unix://var/run/docker.sock')
def watches(event):
    global index
    if(not scaling_flag):
        if not (zk.exists(event.path)):
            #wkno=event.path[8:]
            #print("replacing worker:",index,flush = True)
            w3_container = client.containers.run('worker:latest', name='worker'+str(index), environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=slave:'+str(index)],
                network = 'Orchestrator', volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}, '/home/ubuntu/Orchestrator/filewrites.txt': {'bind': '/src/filewrites.txt', 'mode': 'ro'} },
                detach=True)
            index += 1
            container_list.append(w3_container)
            worker_status.append(1)
            sleep(10)
            zk.get('/slaves/worker'+str(index-1),watch=watches)
            proc_id.append([get_pids(index-1),index-1])



def scale_in():
    global proc_id
    global scaling_flag
    scaling_flag = 1
    crashid = []
    crashid = proc_id
    crashid.sort(reverse=True)
    deadworker_id=crashid[0][0]
    deadworker=crashid[0][1]
    newproc_id=[]
    for i in proc_id:
        if(i[0]!=deadworker_id):
            newproc_id.append(i)
    proc_id=newproc_id
    subprocess.run(['docker','kill','worker'+str(deadworker)])


def job_function():
    global requests_per_2min
    global index
    global client
    global container_list
    print("hello i'm working ", flush = True)
    res = math.floor((requests_per_2min-1)/20)
    if(res<0):
        res = 0

    curr_workers = len(proc_id)
    var = res + 1
    difference = curr_workers - var

    print("Requests ka value", res, flush = True)
    print("req/2min", requests_per_2min, flush = True)
    requests_per_2min = 0
    if(var > curr_workers):
        for i in range(res):
            print("creating new worker:",index, flush = True)
            w3_container = client.containers.run('worker:latest', name='worker'+str(index), environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=slave:'+str(index)],
                        network = 'Orchestrator', volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}, '/home/ubuntu/Orchestrator/filewrites.txt': {'bind': '/src/filewrites.txt', 'mode': 'ro'} },
                        detach=True)
            index += 1
            container_list.append(w3_container)
            worker_status.append(1)
            newname='worker'+str(index-1)
            sleep(10)
            print(newname)
            zk.get('/slaves/'+newname,watch=watches)
            proc_id.append([get_pids(index-1),index-1])
    else:
        for i in range(difference):
            scale_in()


job = cron.add_job(job_function, 'interval', minutes=2)
cron.start()

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', credentials,heartbeat=0)
connection = pika.BlockingConnection(parameters)

writerchannel = WriterClient(connection)
readerchannel = ReaderClient(connection)


w1_container = client.containers.run('worker:latest', name='worker1', environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=master:1'],
               network = 'Orchestrator', volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}},
               detach=True)
#sleep(10)
#zk.get('/slaves/'+'worker1',watch=watches)
container_list.append(w1_container)
worker_status.append(1)
w2_container = client.containers.run('worker:latest', name='worker2', environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=slave:2'],
               network = 'Orchestrator', restart_policy={'Name': 'always'}, volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}},
               detach=True)
sleep(10)
zk.get('/slaves/'+'worker2',watch=watches)
container_list.append(w2_container)
worker_status.append(1)
proc_id.append([get_pids(2),2])
print(proc_id)
sleep(15)

@app.route("/api/v1/worker/list", methods=["GET"])
def process_ID():
    ascending=[]
    for i in proc_id:
        ascending.append(i[0])


    master_pid = get_pids(1)
    ascending.append(master_pid)

    ascending.sort()
    print(ascending)
    return jsonify(ascending), 200

@app.route("/api/v1/crash/slave", methods=["POST"])
def crashSlave():
    global proc_id
    global scaling_flag
    scaling_flag = 0
    crashid = []
    crashid = proc_id
    crashid.sort(reverse=True)
    deadworker_id=crashid[0][0]
    deadworker=crashid[0][1]
    newproc_id=[]
    for i in proc_id:
        if(i[0]!=deadworker_id):
            newproc_id.append(i)
    proc_id=newproc_id
    #proc_id.pop([deadworker_id,deadworker])
    #data, stat = zk.get('/slaves/worker'+str(deadworker))
    #dats = data.decode('utf-8')

    #zk.delete("/slaves/worker"+str(deadworker), recursive=True)
    subprocess.run(['docker','kill','worker'+str(deadworker)])
    '''
    w3_container = client.containers.run('worker:latest', name='worker'+str(index), environment=['TEAM_NAME=CC_0014_0025_0121_0183', 'WORKER_NAME=slave:'+str(index)],
                    network = 'Orchestrator', volumes={'/home/ubuntu/Orchestrator/workers/worker.py': {'bind': '/src/worker.py', 'mode': 'ro'}, '/home/ubuntu/Orchestrator/filewrit$
                    detach=True)
    index += 1
    container_list.append(w3_container)
    worker_status.append(1)
    newname='worker'+str(index-1)
    sleep(10)
    print(newname)
    zk.get('/slaves/'+newname,watch=watches)
    proc_id.append([get_pids(index-1),index-1])
    '''
    #print(dats)
    return flask.Response(json.dumps([deadworker_id]), status=200, mimetype='application/json')

@app.route("/api/v1/db/clear", methods=["POST"])
def clear_db():
    table = "users"
    where = ""
    create_row_data = {"table":table, "where":where, "del":1}
    response = writerchannel.call(json.dumps(create_row_data), 'worker'+master)

    table = "rides"
    where = ""
    create_row_data = {"table":table, "where":where, "del":1}
    response = writerchannel.call(json.dumps(create_row_data), 'worker'+master)

    table = "uride"
    where = ""
    create_row_data = {"table":table, "where":where, "del":1}
    response = writerchannel.call(json.dumps(create_row_data), 'worker'+master)

    d = dict()
    return jsonify(d), 200

@app.route("/api/v1/db/write", methods=["POST", "DELETE"])
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

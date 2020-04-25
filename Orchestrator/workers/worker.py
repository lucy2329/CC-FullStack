#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import pika
import requests
import sqlite3 
from sqlite3 import Error
import re
import csv
import subprocess

worker_name = subprocess.check_output('printenv WORKER_NAME', shell=True).decode('utf-8')[:-1]

database = r"pythonsqlite.db"

worker_id = int(worker_name.split(":")[1])

create_rides_table = """CREATE TABLE IF NOT EXISTS rides (
    ride_num INTEGER PRIMARY KEY, created_by text NOT NULL, timestamp text NOT NULL,source INTEGER NOT NULL, destination INTEGER NOT NULL);"""     
create_userride_table = """CREATE TABLE IF NOT EXISTS uride (num INTEGER NOT NULL, uname text NOT NULL, FOREIGN KEY (num) REFERENCES rides(ride_num)); """
create_count_table = """CREATE TABLE IF NOT EXISTS count (requests INTEGER); """
create_users_table = """CREATE TABLE IF NOT EXISTS users (
    username text PRIMARY KEY, password char(40));"""
def create_connection(db_file):
    conn = None
    conn = sqlite3.connect(db_file)
    return conn

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
    except Error as e:
        print(e)

def get_list(s):
    x = s[1:len(s) - 1]
    x = x.split(",")
    return x

conn = create_connection(database)
if conn is not None:
    create_table(conn, create_rides_table)
    create_table(conn, create_userride_table)
    create_table(conn, create_count_table)
    create_table(conn, create_users_table)
else:
    print("Error! Cannot create the database connection")


def dataReplicate(body):
    print('Replicating data in:', worker_name, flush = True)
    data = get_list(body["insert"])
    columns = get_list(body["columns"])
    table = body["table"]
    types = get_list(body["types"])
    database = r"pythonsqlite.db"
    conn = create_connection(database)
    query = "INSERT INTO " +table + "("
    for x in columns:
        query = query + x + ","
    query = query[:len(query) - 1] + ") VALUES("
    for x in range(len(data)):
        if(types[x] == "string"):
            query = query + "'" +data[x] + "',"
        else:
            query = query + data[x] + ","
    query = query[:len(query) - 1] + ");"
    print(query)
    c = conn.cursor()
    c.execute(query)
    conn.commit()

if(worker_id > 2):
    print("Starting sync", flush = True)
    try:
        f = open("/src/filewrites.txt", "r")
    except:
        print("FILE READ ERROR", flush = True)
    instructions = f.readlines()
    for instruction in instructions:
        dict_to_write = json.loads(instruction)
        print("Writing dict:", dict_to_write, flush = True)
        dataReplicate(dict_to_write)
        print("Writing Completed:", dict_to_write, flush = True)
    print("Sync complete", flush = True)

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', credentials) #,heartbeat=0)
connection = pika.BlockingConnection(parameters)

writechannel = connection.channel()
readchannel = connection.channel()
syncchannel = connection.channel()

readchannel.queue_declare(queue='readQ',durable='True')
writechannel.queue_declare(queue='writeQ',durable='True')

def writeToDatabase(ch, method, props, bodys):
    print("write to db working")

    d = dict()
    print(bodys, type(bodys), "printing body")
    body = json.loads(bodys.decode('utf-8'))
    print('Master write', worker_name, flush = True)
    data = get_list(body["insert"])
    columns = get_list(body["columns"])
    table = body["table"]
    types = get_list(body["types"])
    database = r"pythonsqlite.db"
    conn = create_connection(database)
    query = "INSERT INTO " +table + "("
    for x in columns:
        query = query + x + ","
    query = query[:len(query) - 1] + ") VALUES("
    for x in range(len(data)):
        if(types[x] == "string"):
            query = query + "'" +data[x] + "',"
        else:
            query = query + data[x] + ","
    query = query[:len(query) - 1] + ");"
    print(query)
    c = conn.cursor()
    c.execute(query)
    conn.commit()
    print(bodys, type(bodys), "PRINTING")
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(d))
    ch.basic_ack(delivery_tag = method.delivery_tag)
    syncchannel.basic_publish(exchange='direct_logs', routing_key='syncQ', body=bodys)

def syncToDatabase(ch, method, props, bodys):
    print("sync to db working", flush= True)
    print(bodys, type(bodys), "printing body")
    body = json.loads(bodys.decode('utf-8'))
    print('Master write', worker_name, flush = True)
    data = get_list(body["insert"])
    columns = get_list(body["columns"])
    table = body["table"]
    types = get_list(body["types"])
    database = r"pythonsqlite.db"
    conn = create_connection(database)
    query = "INSERT INTO " +table + "("
    for x in columns:
        query = query + x + ","
    query = query[:len(query) - 1] + ") VALUES("
    for x in range(len(data)):
        if(types[x] == "string"):
            query = query + "'" +data[x] + "',"
        else:
            query = query + data[x] + ","
    query = query[:len(query) - 1] + ");"
    print(query)
    c = conn.cursor()
    c.execute(query)
    conn.commit()
    print(bodys, type(bodys), "PRINTING")

def readFromDatabase(ch, method, props, bodys):
    print('Performing read', flush = True)
    body = json.loads(bodys.decode('utf-8'))
    table = body["table"]
    columns = get_list(body["columns"])
    where = body["where"]
    database = r"pythonsqlite.db"
    conn = create_connection(database)
    query = "SELECT "
    for x in columns:
        query = query + x + ","
    query = query[:len(query) - 1] + " FROM " + table 
    if(len(where) != 0):
        query = query + " WHERE " + where + ";"
    print(query)
    c = conn.cursor()
    c.execute(query)
    rows = c.fetchall()
    print(rows)
    x = json.dumps({"results": rows}).encode('utf-8')  
    readchannel.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=x)
    ch.basic_ack(delivery_tag = method.delivery_tag)


if(worker_name.split(":")[0]=='master'):
    print("THIS IS MASTER", worker_name, flush = True)
    writechannel.basic_consume(queue='writeQ', on_message_callback=writeToDatabase)
    syncchannel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    writechannel.start_consuming()
else:
    print("THIS IS SLAVE", worker_name, flush = True)
    readchannel.basic_consume(queue='readQ', on_message_callback=readFromDatabase)
    syncchannel.exchange_declare(exchange='direct_logs', exchange_type='direct')
    result = syncchannel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    syncchannel.queue_bind(exchange='direct_logs', queue=queue_name, routing_key='syncQ')
    syncchannel.basic_consume(queue=queue_name, on_message_callback=syncToDatabase, auto_ack=True)
    print("STARTING CONSUMPTIOn", flush = True)
    readchannel.start_consuming()
    syncchannel.start_consuming()
    



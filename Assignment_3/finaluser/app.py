from flask import Flask, render_template, jsonify, request, abort, Response
import requests
import json
import sqlite3 
from sqlite3 import Error
import re
import csv
from datetime import datetime

app = Flask(__name__)
methodList = ["GET", "POST", "PUT", "PATCH", "DELETE", "COPY", "HEAD", "OPTIONS", "LINK", "UNLINK", "PURGE", "LOCK", "UNLOCK", "PROPFIND", "VIEW"]

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

@app.route("/api/v1/db/write", methods=["POST"])
def write_to_db():
    data = get_list(request.get_json()["insert"])
    columns = get_list(request.get_json()["columns"])
    table = request.get_json()["table"]
    types = get_list(request.get_json()["types"])
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
    d = dict()
    return jsonify(d)

@app.route("/api/v1/db/read", methods=["POST"])
def read_from_db():
    print("READING FROM DB")
    table = request.get_json()["table"]
    columns = get_list(request.get_json()["columns"])
    where = request.get_json()["where"]
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
    return jsonify(results = rows)

@app.route("/api/v1/db/delete", methods=["DELETE"])
def delete_from_db():

    table = request.get_json()["table"]
    where = request.get_json()["where"]
    database = r"pythonsqlite.db"
    conn = create_connection(database)
    if(len(where) == 0):
        query = "DELETE FROM " + table + ';'
    else:
        query = "DELETE FROM " + table + " WHERE " + where + ";"
    print(query)
    c = conn.cursor()
    c.execute(query)
    conn.commit()
    d = dict()
    return jsonify(d)

def update(increment):
    if(increment):
        database = r"pythonsqlite.db"
        conn = create_connection(database)
        query = "UPDATE count SET requests = requests + 1"
        c = conn.cursor()
        c.execute(query)
        conn.commit()
    else:        
        database = r"pythonsqlite.db"
        conn = create_connection(database)
        query = "UPDATE count SET requests = 0"
        c = conn.cursor()
        c.execute(query)
        conn.commit()


@app.route("/api/v1/_count", methods=["GET", "DELETE"])
def get_requests():
    if request.method == "GET":
        table = "count"
        columns = "[requests]"
        where = ""
        create_row_data = {"table":table, "columns":columns, "where":where}
        r = requests.post("http://35.171.64.212:80/api/v1/db/read", json = create_row_data)
        lst = r.json()["results"] #[13]
        return jsonify(lst[0]), 200
    
    if request.method == "DELETE":
        update(0)
        d = {}
        return jsonify(d), 200

@app.route("/api/v1/db/clear", methods=["POST"])
def clear_db():
    table = "users"
    where = ""
    create_row_data = {"table":table, "where":where}
    r = requests.delete("http://35.171.64.212:80/api/v1/db/delete", json = create_row_data)
    d = dict()

    return jsonify(d), 200

#@app.route("/api/v1/users", methods=["GET"])
@app.route("/api/v1/users", methods=methodList)
def list_users():
    update(1)
    if(request.method == "GET"):
        table = "users"
        columns = "[username]"
        where = ""
        create_row_data = {"table":table, "columns":columns, "where":where}
        r = requests.post("http://35.171.64.212:80/api/v1/db/read", json = create_row_data)
        d = dict()
        if(len(r.json()["results"]) == 0):
            return jsonify(d),204
        else:
            users = []
            for x in r.json()["results"]:
                users.append(x[0])

            return jsonify(users), 200;
    
    if(request.method == "PUT"):
        username = request.get_json().get("username")
        password = request.get_json().get("password")
        # if both the fields have values 
        if(username and password):
            pattern = re.compile(r'\b[0-9a-fA-F]{40}\b')
            match = re.match(pattern, password)
            table = "users"
            columns = "[username,password]"
            where = "username='" + username + "'"
            create_row_data = {"table":table, "columns":columns, "where":where}
            r = requests.post("http://35.171.64.212:80/api/v1/db/read", json = create_row_data)
            d = dict()
            # check if the password is SHA1 hash hex
            if(match):
                # no other user with same username 
                if(len(r.json()["results"]) == 0):
                    insert = "[" + username + "," + password + "]"
                    columns = "[username,password]"
                    types = "[string,string]"
                    table = "users"
                    create_row_data = {"insert":insert, "columns":columns, "table":table, "types":types}
                    r = requests.post("http://35.171.64.212:80/api/v1/db/write", json = create_row_data)
                    # inserted the username and password into the table 'users'
                    return jsonify(d), 201
                else:
                    # some other person has the same username 
                    abort(400)
            else:
                # password entered is wrong 
                abort(400)
            # if server is not available
            if(r.status_code == 500):
                abort(500)
        else:
            # if username or password hasn't been entered
            abort(400)
    else:
        abort(405)

'''
#@app.route("/api/v1/users", methods=["PUT"])
app.route("/api/v1/users", methods=methodList)
def add_user():
    update(1)
    if (request.method == "PUT"):
        username = request.get_json().get("username")
        password = request.get_json().get("password")
        # if both the fields have values 
        if(username and password):
            pattern = re.compile(r'\b[0-9a-fA-F]{40}\b')
            match = re.match(pattern, password)
            table = "users"
            columns = "[username,password]"
            where = "username='" + username + "'"
            create_row_data = {"table":table, "columns":columns, "where":where}
            r = requests.post("http://35.171.64.212:80/api/v1/db/read", json = create_row_data)
            d = dict()
            # check if the password is SHA1 hash hex
            if(match):
                # no other user with same username 
                if(len(r.json()["results"]) == 0):
                    insert = "[" + username + "," + password + "]"
                    columns = "[username,password]"
                    types = "[string,string]"
                    table = "users"
                    create_row_data = {"insert":insert, "columns":columns, "table":table, "types":types}
                    r = requests.post("http://35.171.64.212:80/api/v1/db/write", json = create_row_data)
                    # inserted the username and password into the table 'users'
                    return jsonify(d), 201
                else:
                    # some other person has the same username 
                    abort(400)
            else:
                # password entered is wrong 
                abort(400)
            # if server is not available
            if(r.status_code == 500):
                abort(500)
        else:
            # if username or password hasn't been entered
            abort(400)
    else:
        abort(405)
'''

#@app.route("/api/v1/users/<username>", methods=["DELETE"])
@app.route("/api/v1/users/<username>", methods=methodList)
def delete_user(username):
    update(1)
    if(request.method == "DELETE"):
        username = str(username)
        table = "users"
        columns = "[username,password]"
        where = "username='" + username + "'"
        create_row_data = {"table":table, "columns":columns, "where":where}
        r = requests.post("http://35.171.64.212:80/api/v1/db/read", json = create_row_data)
        d = dict()
        # no user with the username specified
        if(len(r.json()["results"]) == 0):
            return jsonify(d), 400
        else:
            # remove the username with the username that has been specified
            table = "users"
            where = "username='" + username + "'"
            create_row_data = {"table":table, "where":where}
            r = requests.delete("http://35.171.64.212:80/api/v1/db/delete", json = create_row_data)

            #cascade delete 
            #to get all ride numbers associated w this guy in 2nd table
            table = "rides"
            columns = "[ride_num]"
            where = "created_by='" + username + "'"
            create_row_data = {"table":table, "columns":columns, "where":where}
            r = requests.post("http://U-R-1151372789.us-east-1.elb.amazonaws.com/api/v1/db/read", json = create_row_data)

            rnums = r.json()["results"]
            numbers_to_delete = []
            # here

            for x in rnums:
                numbers_to_delete.append(x[0])

            print("NUMBERS TO BE DELETED (test):", numbers_to_delete)
            #to delete from 2nd table
            table = "rides"
            where = "created_by='" + username + "'"
            create_row_data = {"table":table, "where":where}
            r = requests.delete("http://U-R-1151372789.us-east-1.elb.amazonaws.com/api/v1/db/delete", json = create_row_data)
            
            #to delete from 3rd table
            for myrideid in numbers_to_delete:
                table = "uride"
                where = "num="+str(myrideid)
                create_row_data = {"table":table, "where":where}
                r = requests.delete("http://U-R-1151372789.us-east-1.elb.amazonaws.com/api/v1/db/delete", json = create_row_data)
            
            #also have to delete all rows w that guys username so 
            table = "uride"
            where = "uname='" + username + "'"
            create_row_data = {"table":table, "where":where}
            r = requests.delete("http://U-R-1151372789.us-east-1.elb.amazonaws.com/api/v1/db/delete", json = create_row_data)
            
            return jsonify(d), 200
    else:
        abort(405)

@app.route("/api/v1/health_check", methods=["GET"])
def health():
    return jsonify({}),200



def insert_dummy_value():
    database = r"pythonsqlite.db"
    conn = create_connection(database)
    c = conn.cursor()
    query = "INSERT INTO COUNT(requests) SELECT 0 WHERE NOT EXISTS (SELECT requests FROM count);"
    c.execute(query)
    conn.commit()


database = r"pythonsqlite.db"
create_users_table = """CREATE TABLE IF NOT EXISTS users (
    username text PRIMARY KEY, password char(40));"""

create_count_table = """CREATE TABLE IF NOT EXISTS count (requests INTEGER);"""

conn = create_connection(database)
if conn is not None:
    create_table(conn, create_users_table)
    create_table(conn, create_count_table)
    insert_dummy_value()

else:    
    print("Error! Cannot create the database connection")

if __name__ ==  "__main__":
    app.run(host='0.0.0.0',port=80, debug=True)

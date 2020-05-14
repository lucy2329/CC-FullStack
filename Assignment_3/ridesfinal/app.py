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
        query = "DELETE FROM " + table
    else:
        query = "DELETE FROM " + table + " WHERE " + where + ";"
    print(query)
    c = conn.cursor()
    c.execute(query)
    conn.commit()
    d = dict()
    return jsonify(d)

@app.route("/api/v1/db/clear", methods=["POST"])
def clear_db():
    table = "rides"
    where = ""
    create_row_data = {"table":table, "where":where}
    r = requests.delete("http://18.209.136.80:80/api/v1/db/delete", json = create_row_data)

    table = "uride"
    where = ""
    create_row_data = {"table":table, "where":where}
    r1 = requests.delete("http://18.209.136.80:80/api/v1/db/delete", json = create_row_data)

    d = dict()
    return jsonify(d), 200

def get_curr_time():
    x = datetime.now()
    #code to get curr datetime
    if(x.day < 10):
        day = "0" + str(x.day)
    else:
        day = str(x.day)

    if(x.month < 10): 
        month = "0"+str(x.month)
    else:
        month = str(x.month)

    if(x.second < 10):
        second = "0" + str(x.second)
    else:
        second = str(x.second)

    if(x.minute < 10):
        minute = "0" + str(x.minute)
    else:
        minute = str(x.minute)

    if(x.hour < 10):
        hour = "0" + str(x.hour)
    else:
        hour = str(x.hour)

    datestr = str(x.day) + "-" + month + "-" + str(x.year) + ":" + second + "-" + minute + "-" + hour

    return datestr

def compare_dates(d1, d2):
    date1 = d1.split(":")
    date1_date_part = date1[0].split("-")
    date1_time_part = date1[1].split("-")
    

    date2 = d2.split(":")
    date2_date_part = date2[0].split("-")
    date2_time_part = date2[1].split("-")

    curr = datetime(int(date1_date_part[2]), int(date1_date_part[1]), int(date1_date_part[0]), int(date1_time_part[2]), int(date1_time_part[1]), int(date1_time_part[0]))
    new = datetime(int(date2_date_part[2]), int(date2_date_part[1]), int(date2_date_part[0]), int(date2_time_part[2]), int(date2_time_part[1]), int(date2_time_part[0]))

    return (new > curr)

#@app.route("/api/v1/rides", methods=["POST", "GET"])
@app.route("/api/v1/rides", methods=methodList)
def add_ride():
    update(1)
    if request.method == "POST":
        try:
            testint = 5
            created_by = request.get_json().get("created_by")
            timestamp = request.get_json().get("timestamp")
            source = request.get_json().get("source")
            destination = request.get_json().get("destination")
            try:
                source = str(int(source))
                destination = str(int(destination))
            except:
                d = dict()
                abort(400)
            if(created_by and timestamp and source and destination):
                
                origin = {"Origin": "18.209.136.80"}
                r = requests.get("http://U-R-1151372789.us-east-1.elb.amazonaws.com/api/v1/users", headers = origin)
                d = dict()
                print(type(r))
                #if username doesn't exist return 400
                if created_by not in r.json():
                    abort(400)
                elif(not (int(source) in places and int(destination) in places)):
                    abort(400)
                else:
                    try:
                        date_object = datetime.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")
                        insert = "[" + created_by + "," + timestamp + "," + source +"," + destination + "]"
                        columns = "[created_by,timestamp,source,destination]"
                        types = "[string,string,int,int,string]"
                        table = "rides"
                        create_row_data = {"insert":insert, "columns":columns, "table":table, "types":types}
                        r = requests.post("http://18.209.136.80:80/api/v1/db/write", json = create_row_data)
                        return jsonify(d), 201
                    except:
                        abort(400)    
            else:
                # if either one of the four fields is wrong 
                d = dict()
                abort(400)
        except:
            abort(400)
    elif request.method == "GET":
        try:
            datestr = get_curr_time()
            teststr = 5
            source = request.args.get('source')
            destination = request.args.get('destination')
            try:
                    source = str(int(source))
                    destination = str(int(destination))
            except:
                    d = dict()
                    abort(400)
            if(source and destination):
                d = dict()
                if(not (int(source) in places and int(destination) in places)):
                    abort(400)
                else:
                    table = "rides"
                    columns = "[ride_num,created_by,timestamp]"
                    where = "source=" + source + " AND destination=" + destination 
                    create_row_data = {"table":table, "columns":columns, "where":where}
                    r = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
                    if(len(r.json()["results"]) == 0):
                        return jsonify(d), 204
                    else:
                        final_lst = []
                        lst = r.json()["results"]
                        print(lst)
                        print("here: ", lst[0][2], datestr)
                        for x in range(len(lst)):
                            
                            temp = dict()
                            temp["rideId"] = lst[x][0]
                            #temp["username"] = "{" + lst[x][1] + "}"
                            temp["username"] = lst[x][1]
                            temp["timestamp"] = lst[x][2]
                            
                            if(compare_dates(datestr, str(lst[x][2]))):
                                final_lst.append(temp)
                    return json.dumps(final_lst)
            else:
                abort(400)
        except:
            abort(400)
    else:
        d = dict()
        abort(405)

#@app.route("/api/v1/rides/<rideId>", methods=["GET","POST","DELETE"])
@app.route("/api/v1/rides/<rideId>", methods=methodList)
def get_ride_details(rideId):
    update(1)    

    if request.method == "GET":
        try:
            try:
                rideId = str(int(rideId))
            except:
                abort(400)
            table = "rides"
            columns = "[ride_num,created_by,timestamp,source,destination]"
            where = "ride_num=" + rideId
            create_row_data = {"table":table, "columns":columns, "where":where}
            r = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
            r1 = r.json()["results"]
            table = "uride"
            columns = "[uname]"
            where = "num=" +rideId
            create_row_data = {"table":table, "columns":columns, "where":where}
            r = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
            d = dict()
            if(len(r1) == 0):
                return jsonify(d), 400
            final_dict = dict()
            r1 = r1[0]
            r2 = r.json()["results"]
            print(r2)
            print(r1)
            final_dict["rideId"] = rideId
            final_dict["created_by"] = r1[1]
            users = []
            for x in r2:
                #users.append("{" + x[0] + "}")
                users.append(x[0])
            final_dict["users"] = users
            final_dict["timestamp"] = r1[2]
            final_dict["source"] = str(r1[3])
            final_dict["destination"] = str(r1[4])
            return jsonify(final_dict), 200
        except:
            abort(400)
    elif request.method == "POST":
        try:
            rideId = str(int(rideId))
        except:
            abort(400)
        try:
            table = "rides" 
            columns = "[ride_num,created_by]"
            where = "ride_num=" +rideId
            create_row_data = {"table":table, "columns":columns, "where":where}
            r = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
            d = dict()
            if(len(r.json()["results"]) == 0):
                return jsonify(d), 204 
            username = request.get_json().get('username')
            print(username)
            if(r.json()["results"][0][1] == username):
                abort(400)

            origin = {"Origin": "18.209.136.80"}
            r = requests.get("http://U-R-1151372789.us-east-1.elb.amazonaws.com/api/v1/users", headers = origin)
            if username in r.json():
                table = "uride" 
                #table="rides"
                #columns="[created_by]"
                columns = "[uname]"
                where = "uname='" + username + "' AND num=" +rideId
                #where = "ride="+rideId
                create_row_data = {"table":table, "columns":columns, "where":where}
                table1 = "rides"
                columns1 = "[created_by]"
                where1 = "ride_num="+rideId
                create_row_data1 = {"table":table1, "columns":columns1, "where":where1}
                r = requests.post(r"http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
                r1 = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data1)
                if((len(r.json()["results"])==0) and (len(r1.json()["results"]))):
                    print("check")
                    insert = "[" + str(rideId) + "," + username + "]"
                    columns = "[num,uname]"
                    types = "[int,string]"
                    table = "uride"
                    create_row_data = {"insert":insert, "columns":columns, "table":table, "types":types}
                    r = requests.post("http://18.209.136.80:80/api/v1/db/write", json = create_row_data)
                    return jsonify(d), 200
                else:
                    abort(400)
            else:
                abort(400)
        except:
            abort(400)
    elif request.method == "DELETE":
        try:
            try:
                rideId = str(int(rideId))
            except:
                abort(400)
            d = dict()
            table = "rides" 
            columns = "[created_by]"
            where = "ride_num=" + str(rideId)
            create_row_data = {"table":table, "columns":columns, "where":where}
            r = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
            if(len(r.json()["results"]) == 0):
                return jsonify(d), 400
            table = "uride"
            where = "num=" + str(rideId)
            create_row_data = {"table":table, "where":where}
            r = requests.delete("http://18.209.136.80:80/api/v1/db/delete", json = create_row_data)
            table = "rides"
            where = "ride_num=" + str(rideId)
            create_row_data = {"table":table, "where":where}
            r = requests.delete("http://18.209.136.80:80/api/v1/db/delete", json = create_row_data)
            return jsonify(d), 200
        except:
            abort(400)
    else:
        abort(405)

#@app.route("/api/v1/rides/count", methods = ["GET"])
@app.route("/api/v1/rides/count", methods = methodList)
def get_ride_count():
    update(1)
    if(request.method == "GET"):        
        table = "rides"
        columns = "[ride_num,created_by,timestamp,source,destination]"
        where = ""
        create_row_data = {"table":table, "columns":columns, "where":where}
        r = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
        return jsonify([len(r.json()["results"])]), 200
    else:
        abort(405)


@app.route("/api/v1/health_check", methods=["GET"])
def health():
    return jsonify({}),200

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
        r = requests.post("http://18.209.136.80:80/api/v1/db/read", json = create_row_data)
        lst = r.json()["results"] #[13]
        return jsonify(lst[0]), 200
    
    if request.method == "DELETE":
        update(0)
        d = {}
        return jsonify(d), 200


def insert_dummy_value():
    database = r"pythonsqlite.db"
    conn = create_connection(database)
    c = conn.cursor()
    query = "INSERT INTO COUNT(requests) SELECT 0 WHERE NOT EXISTS (SELECT requests FROM count);"
    c.execute(query)
    conn.commit()


database = r"pythonsqlite.db"
create_rides_table = """CREATE TABLE IF NOT EXISTS rides (
    ride_num INTEGER PRIMARY KEY, created_by text NOT NULL, timestamp text NOT NULL,source INTEGER NOT NULL, destination INTEGER NOT NULL);"""     
create_userride_table = """CREATE TABLE IF NOT EXISTS uride (num INTEGER NOT NULL, uname text NOT NULL, FOREIGN KEY (num) REFERENCES rides(ride_num)); """
create_count_table = """CREATE TABLE IF NOT EXISTS count (requests INTEGER); """


conn = create_connection(database)
if conn is not None:
    create_table(conn, create_rides_table)
    create_table(conn, create_userride_table)
    create_table(conn, create_count_table)
    insert_dummy_value()
else:
    print("Error! Cannot create the database connection")   

places = []
with open('AreaNameEnum.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        if(row[0] != 'Area No'):
            places.append(int(row[0]))
if __name__ ==  "__main__":
    app.run(host='0.0.0.0',debug=True, port=80)
    

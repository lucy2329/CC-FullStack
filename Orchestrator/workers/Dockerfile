FROM nouchka/sqlite3:latest

COPY run_worker.sh /docker-entrypoint-initdb.d/

RUN apt update && apt install -y python3 python3-pip

RUN pip3 install flask pika sqlalchemy

ENTRYPOINT ["python3","/src/worker.py"]

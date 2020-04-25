#!/bin/bash

until nouchka/sqlite3 --eval "print(\"waited for connection\")"
  do
    sleep 3
  done
/usr/bin/python3 /src/worker.py
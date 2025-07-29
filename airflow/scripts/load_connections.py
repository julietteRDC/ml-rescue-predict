import json
from airflow.models.connection import Connection
from airflow import settings

with open("/opt/airflow/connections.json") as f:
    conns = json.load(f)

session = settings.Session()
for c in conns:
    conn = Connection(**c)
    session.merge(conn)
session.commit()

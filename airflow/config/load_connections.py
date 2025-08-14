import json
from airflow import settings
from airflow.models import Connection

with open("/opt/airflow/secrets/connections.json") as f:
    connections = json.load(f)
    session = settings.Session()
    for conn in connections:
        # Remove None values for SQLAlchemy compatibility
        conn = {k: v for k, v in conn.items() if v is not None}
        c = Connection(**conn)
        # Remove existing connection if present
        session.query(Connection).filter(Connection.conn_id == c.conn_id).delete()
        session.add(c)
    session.commit()
    session.close()

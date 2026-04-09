import json
from airflow.models import Connection

conn = Connection(
    conn_id="spark_default",
    conn_type="spark",
    host="spark-master",
    port=7077,
    extra=json.dumps({
        "deploy_mode": "client",
        "spark_home": "/opt/spark",
        "conf": {
            "spark.executor.memory": "2g",
            "spark.executor.cores": 2,
        }
    })
)

print(f"AIRFLOW_CONN_{conn.conn_id.upper()}='{conn.get_uri()}'")

Create the Spark connection

Airflow UI → **Admin → Connections → +**

Fill exactly:

- **Conn Id**: `spark_standalone`
- **Conn Type**: `Spark`
- **Host**: spark://spark-master
- **Port**: `7077`
- Leave everything else empty

#!bin/bash
docker exec -it spark-master bash -c '\
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf "spark.driver.extraClassPath=/opt/extra-jars/*" \
    --conf "spark.executor.extraClassPath=/opt/extra-jars/*" \
    /opt/airflow/spark/consumer_to_snowflake.py \
'


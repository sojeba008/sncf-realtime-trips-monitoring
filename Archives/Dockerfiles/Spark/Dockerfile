##VERSION: 1.0.0
FROM bitnami/spark:3.5.0

USER root
RUN install_packages curl

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY consumer_to_deltalake_siri_lite_version.py /app/

CMD ["spark-submit", "--packages", "io.delta:delta-spark_2.12:3.1.0", "/app/consumer_to_deltalake_siri_lite_version.py"]

FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    libsasl2-dev \
    libssl-dev \
    && apt-get clean

# COPY entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# ENTRYPOINT ["/entrypoint.sh"]
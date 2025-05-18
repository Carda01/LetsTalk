FROM python:3.13.3-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

RUN mkdir -p gcs/

COPY gcs-connector-hadoop.jar gcs/
COPY gcs.json gcs/

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY setup_spark.py .
RUN python setup_spark.py

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

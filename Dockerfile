FROM apache/airflow:2.10.5

USER root

# Install Java (required for PySpark)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/default-java
ENV PATH $PATH:$JAVA_HOME/bin

USER airflow

RUN pip install --upgrade pip

# Install Delta Lake and dependencies
RUN pip install pyspark==3.5.5 delta-spark==3.3.0 pandas==2.2.3 pyarrow==19.0.1 requests newsapi-python

# Set environment variables for PySpark
ENV PYTHONPATH "${PYTHONPATH}:/home/airflow/.local/lib/python3.13/site-packages"
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

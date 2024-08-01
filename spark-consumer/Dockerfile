FROM bitnami/spark:latest
WORKDIR /app
RUN pip install pyspark
COPY . /app
USER root
RUN install_packages curl
USER 1001
RUN curl https://jdbc.postgresql.org/download/postgresql-42.7.3.jar --output /opt/bitnami/spark/jars/postgresql-42.7.3.jar
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "app.py"]
FROM openjdk:8

# Spark
ENV SPARK_VERSION 2.1.0
ENV SPARK_HADOOP hadoop2.6
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.6.tgz | tar -xz -C /usr/local
RUN cd /usr/local && ln -s ./spark-$SPARK_VERSION-bin-$SPARK_HADOOP spark
ENV SPARK_HOME /usr/local/spark

ADD data/training-data.txt /opt/
ADD target/scala-2.11/tweets-analyzer-assembly-1.0.jar /opt/

# Run Spark jobs as `root` user.
USER root

# Working directory is set to the home folder of `root` user.
WORKDIR /root

# Expose ports for monitoring.
# SparkContext web UI on 4040 -- only available for the duration of the application.
# Spark master’s web UI on 8080.
# Spark worker web UI on 8081.
EXPOSE 4040 8080 8081


CMD ["/bin/bash"]
FROM apache/hive:3.1.3

USER root

# Install tools
RUN apt-get update && apt-get install -y wget

# Add Postgres JDBC driver
RUN wget -P /opt/hive/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Add Hadoop AWS and AWS SDK (for S3A support)
RUN mkdir -p /opt/hadoop/share/hadoop/tools/lib && \
    wget -P /opt/hadoop/share/hadoop/tools/lib https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    wget -P /opt/hadoop/share/hadoop/tools/lib https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Optional: also include in Hive auxlib
RUN mkdir -p /opt/hive/auxlib && \
    cp /opt/hadoop/share/hadoop/tools/lib/*.jar /opt/hive/auxlib/

ENV HADOOP_CLASSPATH="/opt/hadoop/share/hadoop/tools/lib/*"
ENV HIVE_AUX_JARS_PATH="/opt/hive/auxlib"

USER hive

# Default command: start metastore (can override)
CMD ["hive", "--service", "metastore"]


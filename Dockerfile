FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-11-jdk-headless python3 python3-pip curl ca-certificates \
      procps iputils-ping net-tools nano \
    && rm -rf /var/lib/apt/lists/*

# Hadoop 3.3.6: dùng file tar có sẵn
COPY hadoop-3.3.6.tar.gz /tmp/
RUN tar -xzf /tmp/hadoop-3.3.6.tar.gz -C /opt \
    && mv /opt/hadoop-3.3.6 /opt/hadoop \
    && rm -rf /tmp/hadoop-3.3.6.tar.gz /opt/hadoop/share/doc

# Spark 3.5.1: dùng lại bản pip có sẵn trong máy
COPY spark-pip/ /opt/spark/

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    HADOOP_HOME=/opt/hadoop \
    SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    PYSPARK_PYTHON=python3 \
    HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root
ENV PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data

# Cấu hình đặt cuối để sửa file không phải build lại các lớp nặng ở trên
COPY conf/hadoop/ /opt/hadoop/etc/hadoop/
COPY conf/spark/ /opt/spark/conf/
COPY scripts/entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh /opt/spark/conf/spark-env.sh

ENTRYPOINT ["/opt/entrypoint.sh"]

#!/bin/bash
set -e
case "$1" in
  namenode)
    if [ ! -d /hadoop/dfs/name/current ]; then
      echo ">>> Format NameNode (lần đầu)"
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode ;;
  datanode)        exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager)     exec yarn nodemanager ;;
  spark-master)
    exec spark-class org.apache.spark.deploy.master.Master \
      --host spark-master --port 7077 --webui-port 8080 ;;
  spark-worker)
    exec spark-class org.apache.spark.deploy.worker.Worker \
      --webui-port 8081 spark://spark-master:7077 ;;
  spark-history)
    until hdfs dfs -mkdir -p /spark-logs 2>/dev/null; do
      echo "Chờ HDFS sẵn sàng..."; sleep 3
    done
    exec spark-class org.apache.spark.deploy.history.HistoryServer ;;
  client) exec sleep infinity ;;
  *) exec "$@" ;;
esac

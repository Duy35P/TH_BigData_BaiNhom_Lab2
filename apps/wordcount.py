import sys
import time
import re
from pyspark.sql import SparkSession

if __name__ == "__main__":
    hold_sec = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    spark = SparkSession.builder.appName("WordCount-Movies").getOrCreate()
    sc = spark.sparkContext

    input_path = "hdfs://namenode:9000/bigdata/input/movies.csv"
    output_path = "hdfs://namenode:9000/bigdata/output/wordcount"

    lines = sc.textFile(input_path)
    header = lines.first()
    data = lines.filter(lambda row: row != header)

    words = data.flatMap(lambda row: [w.lower() for w in re.split(r"[^a-zA-Z0-9]+", row) if len(w) > 1 and not w.isdigit()])
    counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)

    # Xóa thư mục output cũ nếu tồn tại
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI("hdfs://namenode:9000"), sc._jsc.hadoopConfiguration())
    out_p = Path(output_path)
    if fs.exists(out_p):
        fs.delete(out_p, True)

    counts.saveAsTextFile(output_path)

    top15 = counts.take(15)
    print("=== TOP 15 WORDS IN MOVIES.CSV ===")
    for word, cnt in top15:
        print(f"  {word:<15} : {cnt}")

    if hold_sec > 0:
        print(f"Holding Spark UI (port 4040) open for {hold_sec}s...")
        time.sleep(hold_sec)

    spark.stop()

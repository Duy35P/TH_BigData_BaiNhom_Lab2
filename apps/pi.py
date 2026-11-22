import sys
import time
from random import random
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    slices = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    n = 100000 * slices

    spark = SparkSession.builder.appName(f"SparkPi-partitions-{slices}").getOrCreate()
    sc = spark.sparkContext

    t0 = time.time()
    def f(_: int) -> int:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = sc.parallelize(range(1, n + 1), slices).map(f).reduce(add)
    elapsed = time.time() - t0
    pi_val = 4.0 * count / n
    print(f"=== [SparkPi] partitions={slices} | samples={n} | Pi is roughly {pi_val:.6f} | elapsed={elapsed:.2f}s ===")
    spark.stop()

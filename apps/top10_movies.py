import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    ratings_file = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000/bigdata/input/ratings_1m.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "hdfs://namenode:9000/bigdata/output/top10_movies.parquet"

    spark = SparkSession.builder.appName(f"Top10Movies-{ratings_file.split('/')[-1]}").getOrCreate()
    t0 = time.time()

    movies_df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://namenode:9000/bigdata/input/movies.csv")
    ratings_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ratings_file)

    stats_df = (
        ratings_df.groupBy("movieId")
        .agg(
            F.count("rating").alias("rating_count"),
            F.round(F.avg("rating"), 4).alias("avg_rating")
        )
        .filter(F.col("rating_count") >= 50)
    )

    top10_df = (
        stats_df.join(movies_df, on="movieId", how="inner")
        .select("movieId", "title", "genres", "rating_count", "avg_rating")
        .orderBy(F.col("avg_rating").desc(), F.col("rating_count").desc())
        .limit(10)
    )

    top10_df.show(10, truncate=False)
    top10_df.write.mode("overwrite").parquet(output_path)

    elapsed = time.time() - t0
    print(f"=== Completed Top10Movies on {ratings_file} in {elapsed:.2f}s -> saved to {output_path} ===")
    spark.stop()

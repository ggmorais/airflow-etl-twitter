import os
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as fn


def get_tweets_data(df):
    return df.select(fn.explode("data")).select("col.*", "col.public_metrics.*").drop("public_metrics")

def get_users_data(df):
    return df.select(fn.explode("includes.users")).select("col.*")

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

def twitter_transform(spark, src, dest, process_date):
    df = spark.read.format("json").load(src)

    table_dest = os.path.join(dest, "{table_name}", "process_date={process_date}")

    df_tweets = get_tweets_data(df)
    df_users = get_users_data(df)

    export_json(df_tweets, table_dest.format(table_name="tweets", process_date=process_date))
    export_json(df_users, table_dest.format(table_name="users", process_date=process_date))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("twitter_transformation").getOrCreate()

    parser = argparse.ArgumentParser(description="Spark Twitter Transformation")
    parser.add_argument("--src", help="Source directory", required=True)
    parser.add_argument("--dest", help="Destination directory", required=True)
    parser.add_argument("--process-date", help="Process date", required=True)

    args = parser.parse_args()

    twitter_transform(
        spark, 
        args.src, 
        args.dest,
        args.process_date
    )

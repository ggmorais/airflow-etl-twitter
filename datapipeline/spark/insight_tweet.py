from pyspark.sql import SparkSession
import pyspark.sql.functions as fn


if __name__ == "__main__":
    spark = SparkSession.builder.appName("insight_tweet").getOrCreate()

    tweet = spark.read.json("/home/gus/cursos/airflow_alura/datapipeline/datalake/silver/twitter_aluraonline/tweets")
    
    alura = tweet\
        .where("author_id == '1566580880'")\
        .select("author_id", "conversation_id")

    tweet = tweet.alias("tweet")\
        .join(
            alura.alias("alura"),
            [
                alura.author_id != tweet.author_id,
                alura.conversation_id == tweet.conversation_id
            ],
            "left"
        ).withColumn(
            "alura_conversation",
            fn.when(fn.col("alura.conversation_id").isNotNull(), 1).otherwise(0)
        ).withColumn(
            "reply_alura",
            fn.when(fn.col("tweet.in_reply_to_user_id") == "1566580880", 1).otherwise(0)
        ).groupBy(fn.to_date("tweet.created_at").alias("created_date"))\
        .agg(
            fn.countDistinct("id").alias("n_tweets"),
            fn.countDistinct("tweet.conversation_id").alias("n_conversation"),
            fn.sum("alura_conversation").alias("alura_conversation"),
            fn.sum("reply_alura").alias("reply_alura")
        ).withColumn("weekday", fn.date_format("created_date", "E"))

    tweet.coalesce(1).write.json("/home/gus/cursos/airflow_alura/datapipeline/datalake/gold/twitter_aluraonline/tweets_insight")

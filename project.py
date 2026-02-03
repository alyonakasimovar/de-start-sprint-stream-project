#import os

# from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, lit, struct,
    unix_timestamp, current_timestamp, round
)
from pyspark.sql.types import StructType, StructField, StringType, LongType


# Настройки
KAFKA_BOOTSTRAP = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
KAFKA_SECURITY_OPTIONS = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": (
        'org.apache.kafka.common.security.scram.ScramLoginModule required '
        'username="de-student" password="ltcneltyn";'
    ),
}
KAFKA_TOPIC_IN = "alyonakasi_in"
KAFKA_TOPIC_OUT = "alyonakasi_out"

PG_SOURCE = {
    "url": "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de",
    "driver": "org.postgresql.Driver",
    "user": "student",
    "password": "de-student",
    "dbtable": "public.subscribers_restaurants",
}

PG_SINK = {
    "url": "jdbc:postgresql://localhost:5432/de",
    "driver": "org.postgresql.Driver",
    "user": "jovyan",
    "password": "jovyan",
    "dbtable": "public.subscribers_feedback",
}

CHECKPOINT_PATH = "/tmp/chk_restaurant_subscribe_alyonakasi"


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    df_to_pg = df.select(
        col("restaurant_id").cast("string").alias("restaurant_id"),
        col("adv_campaign_id").cast("string").alias("adv_campaign_id"),
        col("adv_campaign_content").cast("string").alias("adv_campaign_content"),
        col("adv_campaign_owner").cast("string").alias("adv_campaign_owner"),
        col("adv_campaign_owner_contact").cast("string").alias("adv_campaign_owner_contact"),
        col("adv_campaign_datetime_start").cast("long").alias("adv_campaign_datetime_start"),
        col("adv_campaign_datetime_end").cast("long").alias("adv_campaign_datetime_end"),
        col("datetime_created").cast("long").alias("datetime_created"),
        col("client_id").cast("string").alias("client_id"),
        col("trigger_datetime_created").cast("int").alias("trigger_datetime_created"),
        lit(None).cast("string").alias("feedback"),
    )

    (df_to_pg.write
        .format("jdbc")
        .option("url", PG_SINK["url"])
        .option("driver", PG_SINK["driver"])
        .option("dbtable", PG_SINK["dbtable"])
        .option("user", PG_SINK["user"])
        .option("password", PG_SINK["password"])
        .mode("append")
        .save()
    )

    # создаём df для отправки в Kafka. Сериализация в json.
    df_to_kafka = (
        df.select(
            to_json(
                struct(
                    col("restaurant_id"),
                    col("adv_campaign_id"),
                    col("adv_campaign_content"),
                    col("adv_campaign_owner"),
                    col("adv_campaign_owner_contact"),
                    col("adv_campaign_datetime_start"),
                    col("adv_campaign_datetime_end"),
                    col("client_id"),
                    col("datetime_created"),
                    col("trigger_datetime_created"),
                )
            ).alias("value")
        )
    )

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    (df_to_kafka.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .options(**KAFKA_SECURITY_OPTIONS)
        .option("topic", KAFKA_TOPIC_OUT)
        .save()
    )

    # очищаем память от df
    df.unpersist()


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
    .options(**KAFKA_SECURITY_OPTIONS) \
    .option('subscribe', KAFKA_TOPIC_IN) \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    restaurant_read_stream_df
    .select(col("value").cast("string").alias("value"))
    .withColumn("json", from_json(col("value"), incomming_message_schema))
    .select("json.*")
    .filter(col("restaurant_id").isNotNull())
    .filter(
        (col("adv_campaign_datetime_start").isNotNull()) &
        (col("adv_campaign_datetime_end").isNotNull())
    )
    .filter(
        lit(current_timestamp_utc) >= col("adv_campaign_datetime_start")
    )
    .filter(
        lit(current_timestamp_utc) <= col("adv_campaign_datetime_end")
    )
)

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .options(PG_SOURCE) \
    .load() \
    .select("client_id", "restaurant_id")

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (
    filtered_read_stream_df
    .join(subscribers_restaurant_df, on="restaurant_id", how="inner")
    .withColumn("trigger_datetime_created", lit(current_timestamp_utc).cast("int"))
)

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start() \
    .awaitTermination()


import os

# from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, lit, struct,
    unix_timestamp, current_timestamp, round
)
from pyspark.sql.types import StructType, StructField, StringType, LongType


# Настройки
KAFKA_BOOTSTRAP = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
KAFKA_SECURITY_OPTIONS = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": (
        'org.apache.kafka.common.security.scram.ScramLoginModule required '
        'username="de-student" password="ltcneltyn";'
    ),
}
KAFKA_TOPIC_IN = "alyonakasi_in"
KAFKA_TOPIC_OUT = "alyonakasi_out"


PG_SOURCE = {
    "url": "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de",
    "driver": "org.postgresql.Driver",
    "user": "student",
    "password": "de-student",
    "dbtable": "public.subscribers_restaurants",
}


PG_SINK = {
    "url": "jdbc:postgresql://localhost:5432/de",
    "driver": "org.postgresql.Driver",
    "user": "jovyan",
    "password": "jovyan",
    "dbtable": "public.subscribers_feedback",
}


CHECKPOINT_PATH = "/tmp/chk_restaurant_subscribe_alyonakasi"


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()


    # записываем df в PostgreSQL с полем feedback
    df_to_pg = df.select(
        col("restaurant_id").cast("string").alias("restaurant_id"),
        col("adv_campaign_id").cast("string").alias("adv_campaign_id"),
        col("adv_campaign_content").cast("string").alias("adv_campaign_content"),
        col("adv_campaign_owner").cast("string").alias("adv_campaign_owner"),
        col("adv_campaign_owner_contact").cast("string").alias("adv_campaign_owner_contact"),
        col("adv_campaign_datetime_start").cast("long").alias("adv_campaign_datetime_start"),
        col("adv_campaign_datetime_end").cast("long").alias("adv_campaign_datetime_end"),
        col("datetime_created").cast("long").alias("datetime_created"),
        col("client_id").cast("string").alias("client_id"),
        col("trigger_datetime_created").cast("int").alias("trigger_datetime_created"),
        lit(None).cast("string").alias("feedback"),
    )


    (df_to_pg.write
        .format("jdbc")
        .option("url", PG_SINK["url"])
        .option("driver", PG_SINK["driver"])
        .option("dbtable", PG_SINK["dbtable"])
        .option("user", PG_SINK["user"])
        .option("password", PG_SINK["password"])
        .mode("append")
        .save()
    )


    # создаём df для отправки в Kafka. Сериализация в json.
    df_to_kafka = (
        df.select(
            to_json(
                struct(
                    col("restaurant_id"),
                    col("adv_campaign_id"),
                    col("adv_campaign_content"),
                    col("adv_campaign_owner"),
                    col("adv_campaign_owner_contact"),
                    col("adv_campaign_datetime_start"),
                    col("adv_campaign_datetime_end"),
                    col("client_id"),
                    col("datetime_created"),
                    col("trigger_datetime_created"),
                )
            ).alias("value")
        )
    )


    # отправляем сообщения в результирующий топик Kafka без поля feedback
    (df_to_kafka.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .options(**KAFKA_SECURITY_OPTIONS)
        .option("topic", KAFKA_TOPIC_OUT)
        .save()
    )


    # очищаем память от df
    df.unpersist()


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])


spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()


# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
    .options(**KAFKA_SECURITY_OPTIONS) \
    .option('subscribe', KAFKA_TOPIC_IN) \
    .load()


# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])


# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
# перенесено в фильтрацию функции filtered_read_stream_df
# current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))


# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    restaurant_read_stream_df
    .select(col("value").cast("string").alias("value"))
    .withColumn("json", from_json(col("value"), incomming_message_schema))
    .select("json.*")
    .filter(col("restaurant_id").isNotNull())
    .filter(
        (col("adv_campaign_datetime_start").isNotNull()) &
        (col("adv_campaign_datetime_end").isNotNull())
    )
    # используем функцию unix_timestamp(current_timestamp()) напрямую в фильтре
    .filter(
        unix_timestamp(current_timestamp()) >= col("adv_campaign_datetime_start")
    )
    .filter(
        unix_timestamp(current_timestamp()) <= col("adv_campaign_datetime_end")
    )
)

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .options(**PG_SOURCE) \
    .load() \
    .select("client_id", "restaurant_id")


# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (
    filtered_read_stream_df
    .join(subscribers_restaurant_df, on="restaurant_id", how="inner")
    .withColumn("trigger_datetime_created", unix_timestamp(current_timestamp()).cast("int"))
)


# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start() \
    .awaitTermination()

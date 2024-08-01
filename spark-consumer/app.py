# docker build -t spark-consumer-image . && docker run -dt spark-consumer-image

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, ArrayType, IntegerType

spark = SparkSession.builder.appName("StockTradeConsumer") \
    .config('spark.jars', '/opt/bitnami/spark/jars/postgresql-42.7.3.jar') \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-container:9092") \
  .option("subscribe", "stock-trades") \
  .load()

df = df.selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("data", ArrayType(StructType([
        StructField("c", ArrayType(StringType(), True), True),
        StructField("p", FloatType(), True),
        StructField("s", StringType(), True),
        StructField("t", LongType(), True),
        StructField("v", FloatType(), True)
    ])), True),
    StructField("type", StringType(), True)
])

parsed_df = df.withColumn("jsonData", from_json(col("value"), schema)) \
    .select(col("jsonData.data").alias("data"), col("jsonData.type").alias("type"))

exploded_df = parsed_df.withColumn("data", explode(col("data"))).select(
    col("data.p").alias("price"),
    col("data.s").alias("symbol"),
    col("data.t").alias("time_stamp"),
    col("data.v").alias("volume"),
    col("type").alias("type")
)

# calculate trade price
exploded_df = exploded_df.withColumn("trade_price", exploded_df.price * exploded_df.volume)

def for_each_batch(df, epoch_id):
    print("Epoch ID: " + str(epoch_id))
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres-container:5432/stock_trades") \
        .option("dbtable", "stock_trades") \
        .option("user", "user") \
        .option("password", "password") \
        .save()
    df.show()

exploded_df.writeStream \
    .foreachBatch(for_each_batch) \
    .trigger(processingTime="5 seconds") \
    .start() \
    .awaitTermination()
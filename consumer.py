import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StringType, ArrayType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, VectorAssembler
from pyspark.ml.classification import LogisticRegression

from pyspark.ml import Pipeline
from IPython.display import display, clear_output
from pyspark.sql.functions import col, lower, regexp_replace, udf


import os
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


HOST = 'localhost'
PORT = 2301

spark = SparkSession.builder.appName("Lab4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

element_schema = StructType()\
    .add("comment", StringType())
batch_schema = ArrayType(element_schema)

raw_df = spark.readStream\
    .format("socket")\
    .option("host", HOST)\
    .option("port", PORT)\
    .load()


json_df = raw_df.select(from_json(col("value"), batch_schema).alias("batch_data"))\
    .select(explode(col("batch_data")).alias("element_data"))\
    .select("element_data.comment")

def train_model():
    sample_data = [
        ("I love this video", 1),
        ("This is so bad", 0),
        ("Amazing content", 1),
        ("Terrible quality", 0),
        ("This is the greatest video I have ever seen", 0)
    ]
    df_sample = spark.createDataFrame(sample_data, ["comment", "label"])

    tokenizer = Tokenizer(inputCol="comment", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="features", numFeatures=1000)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF])
    pipeline_model = pipeline.fit(df_sample)
    df_train = pipeline_model.transform(df_sample)
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    model = lr.fit(df_train)

    return model, pipeline

model, pipeline = train_model()


def process_df(df, epoch_id, model, pipeline):
    if any(row.comment == "DONE" for row in df.collect()):
        print("end signal is recieved")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall("OK200".encode())
        print("end signal was sent to producer!!")
        query.stop()
    else:
        pipeline_model = pipeline.fit(df)
        df_fit = pipeline_model.transform(df)
        df_pred = model.transform(df_fit)
        df_pred = df_pred.select(["comment", "prediction"])
        df_pred.show()

terminate_received = False




query = json_df.writeStream\
    .format("console")\
    .outputMode("append")\
    .foreachBatch(lambda df, epoch_id: process_df(df, epoch_id, model, pipeline))\
    .start()


query.awaitTermination()






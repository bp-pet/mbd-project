# script to merge the tag based features and the non tag based features
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

# Global variables
spark = SparkSession.builder.getOrCreate()


# Schema for loading in the tag-based features
schema = StructType([
    StructField("id_tags", IntegerType(), True),
    StructField("PR", DoubleType(), True),
    StructField("ASR", DoubleType(), True),
    StructField("TCF", DoubleType(), True),
    StructField("VCF", DoubleType(), True),
    StructField("ACF", DoubleType(), True),
    StructField("latf_h", DoubleType(), True),
    StructField("RSR", DoubleType(), True),
])
path_tags = "/user/s1978306/post_tags"
df_tags = spark.read.schema(schema).csv(path_tags)


# load in the non tag based features
path = "/user/s2020343/ntb"

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("num_code_snippet", IntegerType(), True),
    StructField("code_len", IntegerType(), True),
    StructField("num_image", IntegerType(), True),
    StructField("body_len", IntegerType(), True),
    StructField("title_len", IntegerType(), True),
    StructField("end_que_mark", BooleanType(), True),
    StructField("begin_que_word", BooleanType(), True),
    StructField("is_weekend", BooleanType(), True)
])

df = spark.read.schema(schema).csv(path)

# Path
count_path = '/user/s1976427/CountVectors'

df_count = spark.read.json(count_path)


result_df = df_tags.join(df, df.id == df_tags.id_tags, 'inner').drop(col('id'))
result_df = result_df.join(df_count, df_count.id == result_df.id_tags, 'inner').drop(col('features')).drop(col('id_tags'))
result_df.show()

# Write to an output file
path_to_write = "/user/s1978306/all_features"
result_df.coalesce(1).write.format('csv').save(path_to_write)

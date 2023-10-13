# Load the tag based and non-tag based features from csv and add the schemas.

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

path = "/user/s2020343/ntb"

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
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

df.printSchema()

df.show(10)




path2 = "/user/s1978306/merged_data2"

schema = StructType([
    StructField("tags", StringType(), True),
    StructField("PR", DoubleType(), True),
    StructField("ASR", DoubleType(), True),
    StructField("TCF", DoubleType(), True),
    StructField("VCF", DoubleType(), True),
    StructField("ACF", DoubleType(), True),
    StructField("latf_h", DoubleType(), True),
    StructField("RSR", DoubleType(), True),
])

df_tags = spark.read.schema(schema).csv(path2)

df.printSchema()

df.show(10)

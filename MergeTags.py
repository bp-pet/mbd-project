# script to merge the tags on the posts
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

# Global variables
spark = SparkSession.builder.getOrCreate()

# import the tag based features
path_tags = "/user/s1978306/merged_data2"

# import the posts from the last 4 years
path = '/user/s1976427/PostsLast4Years'
df = spark.read.json(path)

# load in the data
df1 = df.select(col('_Id').alias('id')\
        ,col('_Tags').alias('tags_4y')\
        )
# filter null values for the
df1 = df1.filter(df1.tags_4y.isNotNull())
# 'Explode the dataframe' by turning it into an rdd and back to an dataframe
rdd1 = df1.rdd
def filter_tags(str): return str[1:-1].split("><")

rdd2 = rdd1.flatMap(lambda row: [(tag,row[0]) for tag in filter_tags(row[1])])
df1 = rdd2.toDF()
df_4y = df1.select(col('_1').alias('tags_4y')\
        ,col('_2').alias('id')\
        )
# Filter on the specific years

# Schema for loading in the tag-based features
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

df_tags = spark.read.schema(schema).csv(path_tags)

# Join on the tags
result_df = df_4y.join(df_tags, df_4y.tags_4y == df_tags.tags, 'inner')
# delete on of the tag columns
result_df = result_df.drop('tags_4y')
result_df = result_df.drop('tags')
# GroupBy the id and calculate the average value of the tags
result_df2 = result_df.groupBy('id')\
		.avg('PR', 'ASR', 'TCF', 'VCF', 'ACF', 'latf_h', 'RSR')

# save to a file
path_to_write = "/user/s1978306/post_tags"
result_df2.coalesce(1).write.format('csv').save(path_to_write)




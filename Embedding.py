from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp

from pyspark.ml import Pipeline
from sparknlp.annotator import Tokenizer, BertEmbeddings


# Constants
readFormat = 'json'
truncated = True

# Paths
basePath = '/user/s1976427/'
jsonDataPath = basePath + ('Truncated' if truncated else '') + 'Posts/'
xmlDataPath = basePath + ('Truncated' if truncated else '') + 'Posts.xml.gz'

# Global variables
spark = SparkSession.builder.getOrCreate()

# Read
if readFormat == 'xml':
    df = spark.read\
        .format('com.databricks.spark.xml')\
        .options(rowTag='row')\
        .load(xmlDataPath)
else:
    df = spark.read.\
        json(jsonDataPath)


df = df.select(col("_PostTypeId").alias("type"),\
		col("_Title").alias("title"),\
		col("_Body").alias("body"),\
		col("_CreationDate").alias("creation_date")) # take only relevant columns


df = df.limit(10)
df.show()

tokenizer = Tokenizer()\
	.setInputCols(["body"])\
	.setOutputCol("tokens")\
	.fit(df)

pipeline = Pipeline().setStages([tokenizer]).fit(df)

df = pipeline.transform(df)

df.show()

bert = BertEmbeddings.pretrained('bert_base_cased', 'en')\
	.setInputCols(["body"])\
	.setOutputCol("bert")\
	.setCaseSensitive(False)


#df = bert.transform(df)

#df.show()

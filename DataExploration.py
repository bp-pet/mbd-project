from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp


# Constants
readFormat = 'json'
truncated = False

# Paths
basePath = '/user/s1976427/'
jsonDataPath = basePath + ('Truncated' if truncated else '') + 'Posts/'
xmlDataPath = basePath + ('Truncated' if truncated else '') + 'Posts.xml.gz'

# Global variables
spark = SparkSession.builder.getOrCreate()

# Timestamp 4 years ago
fourYearsAgoTimestamp = '2018-01-16T12:00:00.000'

# Read
if readFormat == 'xml':
    df = spark.read\
        .format('com.databricks.spark.xml')\
        .options(rowTag='row')\
        .load(xmlDataPath)
else:
    df = spark.read.\
        json(jsonDataPath)

# Filter last 4 years
df = df.filter(col('_CreationDate') >= fourYearsAgoTimestamp)

df.write.option('compression', 'gzip').json('PostsLast4Years')
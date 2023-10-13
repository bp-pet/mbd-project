from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, monotonically_increasing_id, regexp_replace
from pyspark.ml.stat import Summarizer
from pyspark.ml.feature import CountVectorizer


# Global variables
spark = SparkSession.builder \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.11:2.7.5") \
    .config("spark.driver.memory", "15G") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "15G") \
    .getOrCreate()

basePath = '/user/s1976427/PostsLast4Years/'

# Timestamp 4 years ago
fourYearsAgoTimestamp = '2021-01-16T12:00:00.000'

# Data import and preprocessing

df = spark.read. \
    json(basePath)

# Get only answered questions
df = df.alias('q') \
    .join(df.alias('a'), col('q._AcceptedAnswerId') == col('a._Id'), 'inner')
# Filter last 4 years
#df = df.filter(col('q._CreationDate') >= fourYearsAgoTimestamp)
# Get response time
df = df.withColumn('responseTime',
                   unix_timestamp(col('a._CreationDate').cast('timestamp')) -
                   unix_timestamp(col('q._CreationDate').cast('timestamp'))) \
    .withColumn('responseTime', col('responseTime') / 3600) \
    .filter(col('responseTime') >= 0)
# Get labels from response times. From 0 to 1 hours, label 0, from 1 to more, label 1
df = df.withColumn('label', when((col('responseTime') >= 0) & (col('responseTime') < 1), 0).otherwise(1))

# Rename col
df = df.withColumn('body', col('q._Body'))

# Remove new lines
#df = df.withColumn('body', regexp_replace('body', r'\n', ''))

# Strip code blocks
#df = df.withColumn('body', regexp_replace('body', r'<code>(.*)<\/code>', ''))

# Strip html tags
df = df.withColumn('body', regexp_replace('body', '<[^>]*>', ''))

# Final select
df = df.select(col('q._Id').alias('id'), col('body'), col('label'))

# Nlp

from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF

# Assemble the nlp pipeline
documentAssembler = DocumentAssembler() \
    .setInputCol("body") \
    .setOutputCol("document")
sentenceDetector = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")
tokenizer = Tokenizer() \
  .setInputCols(["sentence"]) \
  .setOutputCol("token")

stopWordsCleaner = StopWordsCleaner()\
      .setInputCols(["token"])\
      .setOutputCol("cleanTokens")\
      .setCaseSensitive(False)

stemmer = Stemmer() \
    .setInputCols(["cleanTokens"]) \
    .setOutputCol("stem")

finisher = Finisher() \
    .setInputCols(["stem"]) \
    .setOutputCols(["finisher"]) \
    .setOutputAsArray(True) \
    .setCleanAnnotations(False)

vectorizer = CountVectorizer(inputCol="finisher", outputCol="features", minDF=2.0)

naiveBayes = NaiveBayes(modelType="multinomial", labelCol="label", featuresCol="features")
rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')

hashingTF = HashingTF(inputCol="finisher", outputCol="rawFeatures")# To generate Inverse Document Frequency
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)# convert labels (string) to integers. Easy to process compared to string.



pipeline = Pipeline().setStages([
    documentAssembler,
    sentenceDetector,
    tokenizer,
    stopWordsCleaner,
    stemmer,
    finisher,
    hashingTF,
    idf,
    rf
])


# Takes dataframe with documents, returns dataframe with document embeddings
def getDocEmbeddings(data):
    result = pipeline.fit(data).transform(data)
    return result.selectExpr("id", "features", "label")

res = getDocEmbeddings(df)
#res.show(20, False)
res.write.option('compression', 'gzip').json('TfIdfVectors')
quit()

data = spark.createDataFrame([["This is a sentence. Also, lorem ipsum dolor."], ["Lili lolo. Lala lulu"]]).toDF("body")

## Fitting the model
(trainingData, testData) = df.randomSplit([0.8, 0.2], seed = 100)
model = pipeline.fit(trainingData)
predictions = model.transform(testData)


evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
with open('Result.txt', 'w') as file:
    file.write("Accuracy of RandomForest is = %g"% (accuracy))

#res.write.option('compression', 'gzip').json('CountVectors')
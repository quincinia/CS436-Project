from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split


sc = SparkContext("local")
spark = SparkSession(sc)

if __name__ == '__main__':

    # Read data
    df = spark.read.text("data/normal-crawl/0222/0.txt")

    # Split data into array of values (however there is only 1 column)
    df = df.select(split(df.value, "\t", 10).alias("value"))

    # Split array of values into columns
    df = df.select(df.value[0].alias('videoID'), df.value[1].alias('uploader'), df.value[2].alias('age'), df.value[3].alias('category'), df.value[4].alias('length'), df.value[5].alias('views'), df.value[6].alias('rate'), df.value[7].alias('ratings'), df.value[8].alias('comments'), df.value[9].alias('relatedIDs'))

    # Split string of relatedIDs into array
    df = df.withColumn('relatedIDs', split(df.relatedIDs, '\t'))

    # Cast the numerical types
    df = df.withColumn('age', df.age.cast('int'))
    df = df.withColumn('length', df.length.cast('int'))
    df = df.withColumn('views', df.views.cast('int'))
    df = df.withColumn('rate', df.rate.cast('float'))
    df = df.withColumn('ratings', df.ratings.cast('int'))
    df = df.withColumn('comments', df.comments.cast('int'))

    print(df.first())




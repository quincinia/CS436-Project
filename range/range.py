from __future__ import annotations
import sys

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split

sc = SparkContext("local")
spark = SparkSession(sc)

# Running instructions: "spark-submit ratings.py 0.txt 5"


def formatData(fileName: str):
    # Read data
    df = spark.read.text(fileName)

    # Split data into array of values (however there is only 1 column)
    df = df.select(split(df.value, "\t", 10).alias("value"))

    # Split array of values into columns
    df = df.select(df.value[0].alias('videoID'), df.value[1].alias('uploader'), df.value[2].alias('age'), df.value[3].alias('category'), df.value[4].alias(
        'length'), df.value[5].alias('views'), df.value[6].alias('rate'), df.value[7].alias('ratings'), df.value[8].alias('comments'), df.value[9].alias('relatedIDs'))

    # Split string of relatedIDs into array
    df = df.withColumn('relatedIDs', split(df.relatedIDs, '\t'))

    # Cast the numerical types
    df = df.withColumn('age', df.age.cast('int'))
    df = df.withColumn('length', df.length.cast('int'))
    df = df.withColumn('views', df.views.cast('int'))
    df = df.withColumn('rate', df.rate.cast('float'))
    df = df.withColumn('ratings', df.ratings.cast('int'))
    df = df.withColumn('comments', df.comments.cast('int'))

    return df


def main(argv):
    # Get args from command line parser
    args = sys.argv

    # Initialize dataframe from dataframe formatter
    df = formatData(args[1])

    # Get k
    k = args[2]

    # Run query
    # Query still needs to be made
    # Ranking by rate * ratings
    df.select(df.videoID, df.rate, df.ratings, df.rate *
              df.ratings).orderBy('(rate * ratings)', ascending=False).show(int(k))


if __name__ == '__main__':
    main(sys.argv)


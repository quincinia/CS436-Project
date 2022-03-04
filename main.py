from __future__ import annotations
import sys

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split, trim, col, explode
from pyspark.sql.types import *

sc = SparkContext("local")
spark = SparkSession(sc)


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


def rangeQuery(dataframe, args):
    category = args[3]
    t1, t2 = args[4], args[5]
    dataframe.filter((col('category') == category) & (col('length') >=
                                                      int(t1)) & (col('length') <= int(t2))).orderBy('length', ascending=False).show(5)


def ratingQuery(dataframe, args):
    k = args[3]
    dataframe.select(dataframe.videoID, dataframe.rate, dataframe.ratings, dataframe.rate *
                     dataframe.ratings).orderBy('(rate * ratings)', ascending=False).show(int(k))


def categoriesQuery(dataframe):
    dataframe.groupBy('category').count().orderBy(
        'count', ascending=False).show()


def viewsQuery(dataframe, args):
    k = args[3]
    dataframe.sort(dataframe.views.desc()).show(int(k))


def userRecommendationQuery(df, args):
    username = args[3]
    df.filter(f"uploader='{username}'").select(
        explode('relatedIDs').alias(f'{username} - Related')).dropDuplicates().show()


def main(argv):

    # Get args from command line parser
    args = sys.argv

    # Initialize dataframe from dataframe formatter
    queryChoice = args[1]
    df = formatData(args[2])

    if(queryChoice == "range"):
        rangeQuery(df, args)
    elif(queryChoice == "ratings"):
        ratingQuery(df, args)
    elif(queryChoice == "categories"):
        categoriesQuery(df)
    elif(queryChoice == "views"):
        viewsQuery(df, args)
    elif(queryChoice == "user-recommendation"):
        userRecommendationQuery(df, args)


if __name__ == '__main__':
    main(sys.argv)

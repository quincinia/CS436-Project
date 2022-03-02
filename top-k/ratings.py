# Import command line parser
# Import DataFrame formatter
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext("local")
spark = SparkSession(sc)

if __name__ == '__main__':
    # Get args from command line parser
    args = []

    # Initialize dataframe from dataframe formatter
    df = args[0]

    # Get k
    k = args[1]

    # Run query
    # Query still needs to be made
    # Ranking by rate * ratings
    df.select(df.videoID, df.rate, df.ratings, df.rate * df.ratings).orderBy('(rate * ratings)', ascending=False).show(k)





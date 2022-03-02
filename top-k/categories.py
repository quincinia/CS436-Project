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

    # Run query
    # Filter columns as needed
    df.groupBy('category').count().orderBy('count', ascending=False).show()





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

    # Get categories
    categories = args[1]

    # Get bounds
    t1, t2 = args[2], args[3]

    # Run query
    # Convert categories list to SQL query
    # Filter columns as needed
    df.filter(f'{categories} AND length >= {t1} AND length <= {t2}').show()



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
    # Is it by straight ratings value or rate * number of ratings?
    # Or something else?





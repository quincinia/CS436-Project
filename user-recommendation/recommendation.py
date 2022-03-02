# Import command line parser
# Import DataFrame formatter
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode

sc = SparkContext("local")
spark = SparkSession(sc)

if __name__ == '__main__':
    # Get args from command line parser
    args = []

    # Initialize dataframe from dataframe formatter
    df = args[0]

    # Get username
    username = args[1]

    # Run query
    # Filter columns as needed
    df.filter(f"uploader='{username}'").select(explode('relatedIDs').alias(f'{username} - Related')).dropDuplicates().show()





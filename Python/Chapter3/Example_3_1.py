from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Example_3_1")
             .getOrCreate())
    # Create a DataFrame
    data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)],
                                    ["name", "age"])
    # Group the same names together, aggregate their ages, and compute an average
    avg_df = data_df.groupBy("name").agg(avg("age"))
    # Show the results of the final execution
    avg_df.show()

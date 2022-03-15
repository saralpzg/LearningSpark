from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

    # Crear la SparkSession
    spark = SparkSession.builder.appName("FlightsApp").getOrCreate()

    schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
    t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
    t_c = spark.createDataFrame(t_list, schema)
    t_c.createOrReplaceTempView("tC")
    # Show the DataFrame
    t_c.show()

    print("Calculate Fahrenheit from Celsius for an array of temperatures")
    spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

    print("Filter temperatures > 38C for array of temperatures")
    spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as highTemp FROM tC""").show()

    print("Is there a temperature of 38C in the array of temperatures?")
    spark.sql("""SELECT celsius, exists(celsius, t -> t = 38) as exactTemp FROM tC""").show()

    print("Calculate average temperature and convert to F")
    spark.sql("""SELECT celsius, reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32)
                 as avgFahrenheit FROM tC""").show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


if __name__ == '__main__':
    # Crear la SparkSession
    spark = SparkSession.builder.appName("FlightsApp").getOrCreate()

    csv_path = "C:\\Users\\sara.lopez\\Desktop\\Formación\\LearningSpark\\data\\departuredelays.csv"
    df = spark.read.format("csv").option("inferSchema", True).option("header", True).load(csv_path)
    df.show()
    df.printSchema()
    # Probamos a reformatear la columna de la fecha
    # Cast la fecha de entero a string
    formato = df.withColumn("DATE_STR", df["date"].cast(StringType())).drop("date")
    formato.printSchema()
    # Cast la fecha de string a date
    formato2 = formato.withColumn("DATE", to_timestamp(col("DATE_STR"), "MMddhhmm")).drop("DATE_STR")
    formato2.printSchema()
    formato2.show()

    # Crear una vista temporal
    df.createOrReplaceTempView("us_delay_flights_tbl")

    # Ejemplos de queries con sintaxis SQL basados en la vista temporal
    spark.sql("""SELECT distance, origin, destination
        FROM us_delay_flights_tbl WHERE distance > 1000
        ORDER BY distance DESC""").show(10)
    # Equivalente trabajando con el DataFrame
    (df.select("distance", "origin", "destination")
        .where(col("distance") > 1000)
        .orderBy(desc("distance"))).show(10)

    # Sentencia CASE
    spark.sql("""SELECT delay, origin, destination,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flights_tbl
        ORDER BY origin, delay DESC""").show(10)


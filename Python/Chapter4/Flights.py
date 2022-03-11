from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

if __name__ == '__main__':
    # Crear la SparkSession
    spark = SparkSession.builder.appName("FlightsApp").getOrCreate()

    csv_path = "C:\\Users\\sara.lopez\\Desktop\\Formación\\LearningSpark\\data\\departuredelays.csv"
    schema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING"
    df = spark.read.format("csv").option("header", True).schema(schema).load(csv_path)
    # Probar a pasarle el esquema de string
    df.show()
    df.printSchema()
    # Probamos a reformatear la columna de la fecha
    # Cast la fecha de string a date
    formato = df.withColumn("date_format", to_timestamp(col("date"), "MMddhhmm")).drop("date")
    formato.printSchema()
    formato.show()

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

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    # Managed table
    schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    flights_df = spark.read.csv(csv_path, schema=schema)
    flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

    # Unmanaged table
    (flights_df
     .write
     .mode("overwrite")
     .option("path", "/tmp/data/us_flights_delay")
     .saveAsTable("us_delay_flights_tbl"))

    df_sfo = spark.sql("SELECT date, delay, origin, destination FROM \
    us_delay_flights_tbl WHERE origin = 'SFO'")
    df_jfk = spark.sql("SELECT date, delay, origin, destination FROM \
    us_delay_flights_tbl WHERE origin = 'JFK'")

    # Crear vistas temporales
    df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

    spark.read.table("us_origin_airport_JFK_tmp_view").show()

    print("Ver los metadatos de Catalog:")
    print(spark.catalog.listDatabases())
    print(spark.catalog.listTables())
    # spark.catalog.listColumns("managed_us_delay_flights_tbl")
    print(spark.catalog.listColumns("us_delay_flights_tbl"))

    print("Proceso inverso: de tablas a DataFrames")
    # DataFrame de las filas de vuelos con destino Atlanta (ATL)
    dest_ATL = spark.sql("SELECT * FROM us_delay_flights_tbl WHERE destination = 'ATL'")
    dest_ATL.show()
    print("------- Guardando el DataFrame en un csv -------")
    dest_ATL.write.format("csv").mode("overwrite")\
        .save("C:\\Users\\sara.lopez\\Desktop\\Formación\\LearningSpark\\Python\\Chapter4\\destATL")

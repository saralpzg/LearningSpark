from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Fire_Incidents")
             .getOrCreate())

    # Definimos el esquema
    fire_schema = StructType([
        StructField('CallNumber', IntegerType(), True),
        StructField('UnitID', StringType(), True),
        StructField('IncidentNumber', IntegerType(), True),
        StructField('CallType', StringType(), True),
        StructField('CallDate', StringType(), True),
        StructField('WatchDate', StringType(), True),
        StructField('CallFinalDisposition', StringType(), True),
        StructField('AvailableDtTm', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zipcode', IntegerType(), True),
        StructField('Battalion', StringType(), True),
        StructField('StationArea', StringType(), True),
        StructField('Box', StringType(), True),
        StructField('OriginalPriority', StringType(), True),
        StructField('Priority', StringType(), True),
        StructField('FinalPriority', IntegerType(), True),
        StructField('ALSUnit', BooleanType(), True),
        StructField('CallTypeGroup', StringType(), True),
        StructField('NumAlarms', IntegerType(), True),
        StructField('UnitType', StringType(), True),
        StructField('UnitSequenceInCallDispatch', IntegerType(), True),
        StructField('FirePreventionDistrict', StringType(), True),
        StructField('SupervisorDistrict', StringType(), True),
        StructField('Neighborhood', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('RowID', StringType(), True),
        StructField('Delay', FloatType(), True)
    ])

    # Leer el DataFrame
    file_path = "C:\\Users\\sara.lopez\\Desktop\\Formación\\LearningSpark\\data\\sf-fire-calls.csv"
    fire_df = spark.read.csv(file_path, header=True, schema=fire_schema)

    # Mostrar el DataFrame y el esquema
    fire_df.show()
    print(fire_df.printSchema())
    print(fire_df.schema)

    # Guardar el DataFrame en un archivo Parquet
    parquet_path = "C:\\Users\\sara.lopez\\Desktop\\Formación\\LearningSpark\\Python\\Chapter3\\Parquet_Fire_Python"
    fire_df.coalesce(2).write.format("parquet").save(parquet_path) # Ahora sólo me escribo 2 archivos

    # how many distinct CallTypes were recorded as the causes of the fire calls?
    fire_df.groupby("CallType").agg(count("CallType").alias("DistinctCallTypes")).show()
    # NO EXISTE countDistinct
    fire_df.select("CallType").where(col("CallType").isNotNull()).distinct()\
        .agg(count("CallType").alias("DistinctCallTypes")).show()

    # Seleccionar elementos NO NULOS y DISTINTOS.
    fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().show(10, False)

    # Renombrar columnas del DataFrame. SON INMUTABLES -> se genera un nuevo DF
    new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
    new_fire_df.select("ResponseDelayedinMins").show()

    # Parsear las columnas con fechas a tipo date (to_timestamp)
    fire_ts_df = (new_fire_df
                  .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                  .drop("CallDate")
                  .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
                  .drop("WatchDate")
                  .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                  .drop("AvailableDtTm"))
    fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show()
    # Ahora podemos hacer consultas con las funciones year(), month() y day()
    fire_ts_df.select(year('IncidentDate')).distinct().orderBy(year('IncidentDate')).show()

    print("LLamada más frecuente:")
    fire_df.select("CallType").where(col("CallType").isNotNull()).groupby("CallType").count()\
        .orderBy(desc("count")).show(1)

    print("Número total de alarmas, tiempo medio de respuesta, tiempo mínimo y máximo")
    (fire_ts_df
        .select(sum("NumAlarms"), avg("ResponseDelayedinMins"),
                min("ResponseDelayedinMins"), max("ResponseDelayedinMins"))
        .show())

    print("Tipos de llamadas en 2018")
    (fire_ts_df.select("CallType", "IncidentDate").where(year("IncidentDate") == "2018").distinct().show())

    print("Meses del año 2018 con mayor número de llamadas")
    (fire_ts_df.select("IncidentDate").where(year("IncidentDate") == "2018")
     .groupBy(month("IncidentDate").alias("DateMonth"), "IncidentDate").count().orderBy(desc("count")).show())
    # Incluir las columnas en la agregación para poder imprimirlas en la tabla de salida

    print("Barrios de San Francisco que generaron mayor número de llamadas en 2018")
    (fire_ts_df.select("Neighborhood").where(year("IncidentDate") == "2018").groupby("Neighborhood")
     .count().orderBy(desc("count")).show())

    print("Qué barrios tuvieron los peores tiempos de respuesta en 2018")
    (fire_ts_df.select("Neighborhood", "ResponseDelayedinMins").where(year("IncidentDate") == "2018")
     .groupby("Neighborhood").avg("ResponseDelayedinMins").orderBy(desc(avg("ResponseDelayedinMins"))).show())

    print("Qué semana de 2018 tuvo más llamadas")
    (fire_ts_df.select("IncidentDate").where(year("IncidentDate") == "2018").groupby(weekofyear("IncidentDate"))
     .count().orderBy(desc("count")).show())

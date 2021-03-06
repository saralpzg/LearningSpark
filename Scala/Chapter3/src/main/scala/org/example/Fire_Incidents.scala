package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Fire_Incidents {
  def main(args : Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[1]")
      .appName("Fire_Incidents")
      .getOrCreate()
    if (args.length < 1) {
      print("Usage: <sf_fire_calls>")
      sys.exit(1)
    }
    // Definimos el esquema
    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    ))

    // Tomamos la ruta del csv de datos de los par??metros
    val file_path = args(0)
    val fireDF = spark.read.schema(fireSchema).option("header", true).csv(file_path)
    // Mostramos los resultados
    fireDF.show()
    println(fireDF.printSchema) // M??s sencillo para ver la estructura
    println(fireDF.schema) // Imprime la definici??n del esquema (c??digo)

    // Guardamos el DataFrame en un archivo Parquet
//    val parquetPath = "C:\\Users\\sara.lopez\\Desktop\\Formaci??n\\LearningSpark\\Scala\\Chapter3\\Parquet_Fire_Scala"
//    fireDF.write.parquet(parquetPath)

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
      .show()

    // Seleccionar elementos NO NULOS y DISTINTOS.
//    fireDF.select("CallType").where($"CallType".isNotNull()).distinct().show(10, false)
    fireDF.select("CallType").distinct().where(col("CallType").isNotNull).show(10, false)

    // Renombrar columnas. SON INMUTABLES -> se genera un nuevo DF
    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF.select("ResponseDelayedinMins").show()
//    fireDF.select("ResponseDelayedinMins").show() // Este c??digo falla porque el DF original no cambia

    // # Parsear las columnas con fechas a tipo date (to_timestamp)
    // M??todo withColumn crea una nueva columna, por lo que hay que hacer un drop de la vieja
    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")
    fireTsDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show()
    // Ahora se pueden invocar los m??todos year(), month(), day()
    fireTsDF.select(year(col("IncidentDate")) as("Years")).distinct().orderBy("Years").show()
//    fireTsDF.select(year($"IncidentDate")).distinct().orderBy(year($"IncidentDate")).show()

    println("Llamada m??s frecuente")
    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(1, false)

    println("N??mero total de alarmas, tiempo medio de respuesta, tiempo m??nimo y m??ximo")
    fireTsDF
      .select(sum("NumAlarms"), avg("ResponseDelayedinMins"),
        min("ResponseDelayedinMins"), max("ResponseDelayedinMins"))
      .show()

    print("Tipos de llamadas en 2018")
    fireTsDF.select("CallType", "IncidentDate").where(year(col("IncidentDate")) === "2018").distinct().show()
  }
}

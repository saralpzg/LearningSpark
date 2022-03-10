package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * @author Sara López Gutiérrez
 */
object Flights {

  def main(args : Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[1]")
      .appName("Fire_Incidents")
      .getOrCreate()

    val csv_path = "C:\\Users\\sara.lopez\\Desktop\\Formación\\LearningSpark\\data\\departuredelays.csv"
    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val df = spark.read.schema(schema).option("header", true).csv(csv_path)
    df.show()

    // Cast la fecha de entero a string
//    val flights_df = df.withColumn("date_str", col("date").cast(StringType())).drop("date")
//    flights_df.printSchema()
    val formato = df.withColumn("Date", to_timestamp(col("date"), "MMddhhmm"))
    formato.show()

    // Crear una vista temporal
    df.createOrReplaceTempView("us_delay_flights_tbl")

    // Ejemplos de queries con sintaxis SQL basados en la vista temporal
    spark.sql("""SELECT distance, origin, destination
        FROM us_delay_flights_tbl WHERE distance > 1000
        ORDER BY distance DESC""").show(10)
    // Equivalente trabajando con el DataFrame
    (df.select("distance", "origin", "destination")
      .where(col("distance") > 1000)
      .orderBy(desc("distance"))).show(10)

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    // Managed table
    val schema_2 = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val flights_df = spark.read.schema(schema_2).option("header", false).csv(csv_path)
//    flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

    // Unmanaged table
      (flights_df
        .write
        .option("path", "/tmp/data/us_flights_delay")
        .saveAsTable("us_delay_flights_tbl"))

    val df_sfo = spark.sql("SELECT date, delay, origin, destination FROM" +
      " us_delay_flights_tbl WHERE origin = 'SFO'")
    val df_jfk = spark.sql("SELECT date, delay, origin, destination FROM " +
      "us_delay_flights_tbl WHERE origin = 'JFK'")

    // Crear vistas temporales
    df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

    spark.read.table("us_origin_airport_JFK_tmp_view").show()

    print("Proceso inverso: de tablas a DataFrames")
    // DataFrame de las filas de vuelos con destino Atlanta (ATL)
    val dest_ATL = spark.sql("SELECT * FROM us_delay_flights_tbl WHERE destination = 'ATL'")
    dest_ATL.show()
    print("------- Guardando el DataFrame en un csv -------")
    dest_ATL.write.format("csv").mode("overwrite")
    .save("C:\\Users\\sara.lopez\\Desktop\\Formación\\LearningSpark\\Scala\\Chapter4\\destATL")

  }
}

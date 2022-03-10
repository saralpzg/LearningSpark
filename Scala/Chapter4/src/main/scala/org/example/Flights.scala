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
    val df = spark.read.schema(schema).format("csv").load(csv_path)
    df.show()

    // Cast la fecha de entero a string
//    val flights_df = df.withColumn("date_str", col("date").cast(StringType())).drop("date")
//    flights_df.printSchema()
  }

}

package org.example

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Example_3_2 {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("Example_3_2")
      .getOrCreate()
    // Create a DataFrame of names and ages
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
    // Group the same names together, aggregate their ages, and compute an average
    val avgDF = dataDF.groupBy("name").agg(avg("age"))
    // Show the results of the final execution
    avgDF.show()
  }
}

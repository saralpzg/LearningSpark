package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @author Sara López Gutiérrez
 */
object Exercises_Chapter5 {

  def main(args : Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[1]")
      .appName("Exercises_Chapter5")
      .getOrCreate()

    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40)
    val t2 = Array(31, 32, 34, 55, 56)
//    val tC = Seq(t1, t2).toDF("celsius")
//    tC.createOrReplaceTempView("tC")
//    tC.show()

  }

}

package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.util.Random._

/**
 * @author Sara López Gutiérrez
 */
object Exercises_Chapter6 {

  def main(args : Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[1]")
      .appName("Exercises_Chapter6")
      .getOrCreate()

    // Clase que define el esquema del DataSet
    case class Usage(uid:Int, uname:String, usage: Int)

    val r = new scala.util.Random(42)
    // Create 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)))
    // Create a Dataset of Usage typed data
    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)
  }

}

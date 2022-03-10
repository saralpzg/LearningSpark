package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Example_3_4 {
  def main(args : Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[1]")
      .appName("Example_3_4")
      .getOrCreate()
    if (args.length <= 0) {
      println("usage Example3_7 <file path to blogs.json>")
      System.exit(1)
    }
    val jsonFile = args(0)
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    // Create a DataFrame by reading from the JSON file
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)
    // Print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)
  }
}

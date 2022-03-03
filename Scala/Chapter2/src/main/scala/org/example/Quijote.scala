package org.example

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Quijote {
  def main(args : Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    val text = spark.read.text("C:\\Users\\sara.lopez\\Desktop\\LearningSpark\\data\\el_quijote.txt")
    // Número de líneas
    val num_lines = text.count()
    println("El fichero del Quijote tiene " + num_lines + " líneas.")

    println("------------------------------------")
    println("Distintas opciones del comando show")
    println("-------------------------------------")
    text.show()
    text.show(numRows = 10)
    text.show(truncate = true)

    println("-------------------------------------")
    println("Otros comandos")
    println("-------------------------------------")
    text.head(10)
    text.take(10)
    text.first()
  }
}

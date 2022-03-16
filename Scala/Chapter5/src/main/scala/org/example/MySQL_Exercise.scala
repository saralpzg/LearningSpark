package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MySQL_Exercise {

  def main(args : Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[1]")
      .appName("MySQL_Exercise")
      .getOrCreate()

    val employees = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "employees")
      .option("user", "root")
      .option("password", "fs5LyPRw")
      .load()
    employees.show(10)

    val department_emp = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "dept_emp")
      .option("user", "root")
      .option("password", "fs5LyPRw")
      .load()
    department_emp.show(10)

    val salaries = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "salaries")
      .option("user", "root")
      .option("password", "fs5LyPRw")
      .load()
    salaries.show(10)

    employees.join(department_emp, employees.col("emp_no") === department_emp.col("emp_no"))
      .join(salaries, employees.col("emp_no") === salaries.col("emp_no"))
      .select("first_name", "last_name", "dept_no", "salary").show()
  }

}

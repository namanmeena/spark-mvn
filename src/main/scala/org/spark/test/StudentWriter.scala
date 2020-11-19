package org.spark.test

import org.apache.spark.sql.SparkSession

/**
 * [[StudentWriter]]
 *
 * Writes 1 million records of a student in 1 file.
 * It takes a path to directory where the student record file will be stored.
 *
 */
object StudentWriter {

  /**
   * Student class holding information about a student.
   *
   * @param rollNumber of the student
   * @param name       of the student
   * @param gender     of the student
   */
  case class Student(rollNumber: Long, name: String, gender: String)

  /**
   * Main function which starts the program
   *
   * @param args arguments to main
   */
  def main(args: Array[String]): Unit = {
    if (!args.isDefinedAt(0))
      throw new IllegalStateException("argument 0 should be present to save the output")

    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df = sc.parallelize(List.range(0, 1000000)
      .map { n => Student(n, "Sam", "M") })
      .toDF("rollNumber", "name", "gender")

    df.repartition(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(args(0))
  }
}

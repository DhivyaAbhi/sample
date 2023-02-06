package awspack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object obj {

  def main(args: Array[String]): Unit = {

    println("===Started===")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header","true")
      .option("fs.s3a.access.key", "AKIAWBEIBQWVKJWP5GGU")
      .option("fs.s3a.secret.key", "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")
      .load("s3a://dhivya-s3/datatxns.txt")

    df.show()
  }

}
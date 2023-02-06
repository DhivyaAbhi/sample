package dslpac

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object obj {

  def main(args: Array[String]): Unit = {

    println("===Started===")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val dtdf = spark.read.format("csv")
      .option("header", "true")
      .load("file:///D:/data/dt.txt")
    dtdf.show()
    println("=====and operator=====")
    val adf = dtdf.filter(
      col("category") === "Gymnastics"
        &&
        col("Spendby") === "cash")
    adf.show()
    println("=====or operator====")
    val odf = dtdf.filter(
      col("category") === "Gymnastics"
        ||
        col("Spendby") === "cash")
    odf.show()
    println("====like operator=====")
    val ldf = dtdf.filter(
      col("product") like "%Gymnastics%")
    ldf.show()
    println("=====isin operator=====")
    val isdf = dtdf.filter(
      col("category") isin ("Exercise", "Gymnastics"))
    isdf.show()
    println("======null operator=====")
    val ndf = dtdf.filter(
      col("id").isNull)
    ndf.show()
    println("=====not null=====")
    val nndf = dtdf.filter(
      !(col("id").isNull))
    nndf.show()
    println("=====SelectExpression=====")
    val sdf = dtdf.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "amount",
      "category",
      "product",
      "Spendby")
    sdf.show()

    println("======Task2=====")
    val usdf = spark.read.format("csv")
      .option("header", "true")
      .load("file:///D:/data/usdata.csv")

    val fdf = usdf.filter(col("state") === "LA"
      &&
      col("age")>10)
    fdf.show()

  }
}
  
package pack4

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

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///D:/data/dt.txt")

    df.show()
    // sdf = df.selectExpr(
    // "id",
    // "split(tdate,'-')[2] as year",
    // "amount",
    //  "category",
    //  "product",
    //  "Spendby",
    //  "case when spendby='cash' then 0 else 1 end as status")

   // val fdf = df.withColumn("tdate", expr("split(tdate,'-')[2]"))
     // .withColumnRenamed("tdate", "year")
      //.withColumn("status", expr("case when spendby='cash' then 0 else 1 end"))

   val finaldf=df.groupBy(col("category"))
              // .agg(sum(col("amount")).as("total"))
               .agg(count(col("category")).as("count"))
               //.orderBy(col("total"))
       
       finaldf.show()
   

  }
}
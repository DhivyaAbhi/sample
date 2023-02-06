package snowflake
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.io.Source

object obj {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val snowval = spark.read.format("snowflake")
      .option("sfURL", "https://zlvocyn-lk25971.snowflakecomputing.com")
      .option("sfAccount","zlvocyn")
      .option("sfUser" , "zeyobronanalytics")
      .option("sfPassword" , "Aditya908")
      .option("sfDatabase" , "zeyodb")
      .option("sfSchema" , "PUBLIC")
      .option("sfRole" , "ACCOUNTADMIN")
      .option("sfWarehouse" , "ZEYOWH")
      .option("dbtable","zeyoin").load()
    snowval.show()

  }

}
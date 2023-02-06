package joinpack

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

    println("=========Started=========")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df1=spark.read.format("csv").option("header","true")
                    .load("file:///D:/data/join1.txt")
    df1.show()
    
    val df2=spark.read.format("csv").option("header","true")
                    .load("file:///D:/data/join2.txt")
    df2.show()
    
    println("====Inner====")
    val joindf=df1.join(df2,df1("txnno")===df2("tno"),"inner").drop("tno")
    joindf.show()
    println("====Left====")
    val ljoin=df1.join(df2,df1("txnno")===df2("tno"),"left").drop("tno")
    ljoin.show()
    println("====Right====")
    val rjoin=df1.join(df2,df1("txnno")===df2("tno"),"right").drop("txnno")
    rjoin.show()
    println("====Full====")
    val fjoin=df1.join(df2,df1("txnno")===df2("tno"),"full")
                .withColumn("txnno",expr("case when txnno is null then tno else txnno end"))
                 .orderBy("txnno")
                 .drop("tno")
    fjoin.show()
    println("======LeftAntiJoin=====")
    val df3=df2.select("tno")
    val antidf=df1.join(df3,df1("txnno")===df3("tno"),"left_anti")
    antidf.show()
    
   
    
  }
  
}
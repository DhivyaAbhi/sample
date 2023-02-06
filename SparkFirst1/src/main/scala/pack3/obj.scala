package pack3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
  
object obj {
  
 def main(args:Array[String]):Unit={
    
    println("===Started===")
  
  val conf=new SparkConf().setAppName("first").setMaster("local[*]")
  val sc=new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val spark=SparkSession.builder().getOrCreate()
  import spark.implicits._
    
    val df=spark.read.format("csv").option("header","true") .load("file:///D:/data/usdata.csv")
     
     df.select("first_name","last_name").show()
     
    // finaldf.write.format("csv").mode("overwrite")
           //.partitionBy("state","county").save("file:///D:/data/usdf")        
         
  }
  
}
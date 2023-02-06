package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj {
   
   def main(args:Array[String]):Unit={
    
    println("===Started===")
  
  val conf=new SparkConf().setAppName("first").setMaster("local[*]")
  val sc=new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val spark=SparkSession.builder().getOrCreate()
  import spark.implicits._
    
  //val df=spark.read.format("json").load("file:///D:/data/devices.json")
  //df.show()
  
  //val pardf=spark.read.format("parquet").load("file:///D:/data/data.parquet")
  //pardf.show()
    
  //val orcdf=spark.read.format("orc").load("file:///D:/data/data.orc")
    //orcdf.show()
   
  //val csvdf=spark.read.format("csv").load("file:///D:/data/datatxns.txt")
   //csvdf.show()
  
  //val json=spark.read.format("json").load("file:///D:/data/devices.json")
  //json.show()
  
  //json.createOrReplaceTempView("jdf")
  //val finaldf=spark.sql("select * from jdf where lat>40")
  //finaldf.show()
  
  //finaldf.write.format("orc").mode("overwrite").save("file:///D:/data/orcdf")
  //println("===Done===")
  
  val avrodf=spark.read.format("avro").load("file:///D:/data/data.avro")
  avrodf.show()
      }
}
package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object obj {
  
  case class schema(id:String,tdate:String,category:String,product:String)
  
   def main(args:Array[String]):Unit={
    
    println("===Started===")
  
  val conf=new SparkConf().setAppName("first").setMaster("local[*]")
  val sc=new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val spark=SparkSession.builder().getOrCreate()
  import spark.implicits._
    println("===FileData===")
    val rawdata=sc.textFile("file:///D:/data/datatxns.txt",1)
    rawdata.foreach(println)
    
    val mapsplit=rawdata.map(x => x.split(","))
    
    println("====Filte 4th Column===")
     
    val schrdd=mapsplit.map(x => schema(x(0),x(1),x(2),x(3)))
    
    val filterrdd=schrdd.filter(x => x.product.contains("Gymnastics"))
    
    filterrdd.foreach(println)
    
    val df=filterrdd.toDF()
    df.show()
    df.write.parquet("file:///D:/data/park")
  }
  
}
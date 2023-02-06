package pack5

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

    //val df = spark.read.format("csv").option("header", "true")
     // .load("file:///D:/data/dtdata.txt")

    //df.show()

   
   // val fdf = Window
        //   .partitionBy($"dept")
        //   .orderBy($"salary" )
  // val fdf1=df.withColumn("col3", dense_rank() over fdf)
                //     .filter("col3=2")
                    // .orderBy("dept")
      //  fdf1.show()
    
   val df = spark.read.format("csv").option("header", "true")
            .load("file:///D:/data/emptable.txt")
       df.show()
       
            
    df.withColumn("Rank",dense_rank().over(Window.orderBy($"salary" desc))).where("Rank=2").show()
    
            
        
     }

}
package pack2
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
  
     val xmldf=spark.read.format("xml").option("rowTag", "book").load("file:///D:/data/books.xml")
     xmldf.show()
     xmldf.printSchema()
  
  //val df=spark.read.format("csv").load("file:///D:/data/datatxns.txt")
  //df.show()
  //df.printSchema()
  //df.write.format("csv").save("file:///D:/data/mcheck")
  //df.write.format("csv").mode("append").save("file:///D:/data/mcheck")
  //df.write.format("csv").mode("overwrite").save("file:///D:/data/mcheck")
  //df.write.format("csv").mode("ignore").save("file:///D:/data/mcheck")
  
  //val df=spark.time(spark.read.format("jdbc")
  //            .option("url","jdbc:mysql://zeyodb.czvjr3tbbrsb.ap-south-1.rds.amazonaws.com/zeyodb")
  //            .option("user","root")
  //           .option("password","Aditya908")
  //           .option("dbtable","kgf")
  //          .option("driver","com.mysql.cj.jdbc.Driver")
  //          .load())
 //df.show()
  }
}
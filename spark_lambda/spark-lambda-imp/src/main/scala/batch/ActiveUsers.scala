package batch

import java.lang.management.ManagementFactory

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ActiveUsers {
  def main(args: Array[String]): Unit = {

    // Specify spark configuration and spark context
    val sc = new SparkContext(init())
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // initialize input RDD

      val sourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\purchase_order_header.tsv"
      val input = sc.textFile(sourceFile)

      //val header = input.first()
      //val rows = input.filter(line => line != header)
   //write file in parquet format
    writeParquet(input.toDF(),"purchase_order_parquet")
    //Read parquet file
    val parquetSourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\purchase_order_parquet"
    val parquetFile = sqlContext.read.parquet(parquetSourceFile)
    //parquetFile.foreach(println)
    //Register temp table
    parquetFile.registerTempTable("purchase_order_tb")
    val activeUsers=sqlContext.sql("select * from purchase_order_tb")
    activeUsers.foreach(println)
  }

  /*
  Function to write parquet files
   */
  def init(): SparkConf = {
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")

    // Check if running from IDE
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "c:\\WinUtils") // required for winutils
      conf.setMaster("local[*]")
    }
   conf
  }
  def writeParquet(dfName:DataFrame, fileName:String):Unit={
    dfName.write
      .option("header", "true")
      .mode("overwrite")
      .save("C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\"+fileName)
    }

}
d
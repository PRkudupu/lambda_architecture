package batch

import java.lang.management.ManagementFactory

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ActiveUsers {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(init())
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    /****
     Read text file from the specified location.
     Add schema to the file
     */
    val sourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\purchase_order_header.tsv"
    val input = sc.textFile(sourceFile)

    val inputDF = input.flatMap { line =>
      //split and assign it to a variable
      val record = line.split("\\t")
      //Validate if the actual input is of the actual length
      if (record.length == 13){
        //flatMap would unbox and return the activity
        Some(domain.PurchaseOrder(record(0), record(1), record(2), record(3), record(4), record(5), record(6),record(7), record(8), record(9), record(10), record(11), record(12)))
      }
      else
        None
    }.toDF()

    /**
     * Write dataframe with schema as parquet file
     */
    writeParquet(inputDF,"purchase_order_parquet")

    /**
     * Read for parquet file
     */

    val parquetSourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\purchase_order_parquet"
    val parquetFile = sqlContext.read
      .option("mergeSchema","true")
      .parquet(parquetSourceFile)
    //Register temp table
    parquetFile.registerTempTable("purchase_order_tb")
    parquetFile.printSchema()
    val activeUsers=sqlContext.sql("""select
                                    * from purchase_order_tb""")
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

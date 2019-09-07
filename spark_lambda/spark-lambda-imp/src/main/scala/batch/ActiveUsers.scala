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
        Some(domain.PurchaseOrder(record(0), record(1), record(2), record(3), record(4), record(5), record(6),record(7),
          record(8), record(9), record(10), record(11), record(12)))
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
    parquetFile.registerTempTable("purchase_order_t")
    parquetFile.printSchema()
    val activeUsers=sqlContext.sql("""select a.order_number
          , a.cosmos_customerid
          , header_purchase_date
           , case when months_between(from_unixtime(unix_timestamp()),header_purchase_date) < 36.0
                then 1 else 0 end as is_36_months
          , case when months_between(from_unixtime(unix_timestamp()),header_purchase_date) < 24.0
                then 1 else 0 end as is_24_months
          , case when months_between(from_unixtime(unix_timestamp()),header_purchase_date) < 12.0
                then 1 else 0 end as is_12_months
           from purchase_order_t a
           join
           (select cosmos_customerid
                , max(header_purchase_date) as max_date
           from
           purchase_order_t
           group by cosmos_customerid) b
           on a.cosmos_customerid=b.cosmos_customerid and a.header_purchase_date=b.max_date""")
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

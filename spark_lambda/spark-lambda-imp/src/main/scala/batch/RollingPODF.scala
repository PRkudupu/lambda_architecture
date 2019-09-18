package batch

import java.lang.management.ManagementFactory

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RollingPO {
  val root ="C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\data\\"
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(init())
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    /****
     * PURCHASE ORDER
     * **********/
    /**
    Read text file from the specified location.
     Add schema to the file
     */
    val po_sourceFile = root +"purchase_order.tsv"
    val po_input = sc.textFile(po_sourceFile)

    val po_inputDF = po_input.flatMap { line =>
      //split and assign it to a variable
      val po_record = line.split("\\t")
      //Validate if the actual input is of the actual length
      if (po_record.length == 13){
        //flatMap would unbox and return the activity
        Some(domain.PurchaseOrder(po_record(0), po_record(1), po_record(2), po_record(3), po_record(4), po_record(5), po_record(6),po_record(7),
          po_record(8), po_record(9), po_record(10), po_record(11), po_record(12)))
      }
      else
        None
    }.toDF()

    /**
     * Write dataframe with schema as parquet file
     */
    writeParquet(po_inputDF,"purchase_order_pq")

    /**
     * Read for parquet file
     */

    val po_sourceFile_pq = root+"purchase_order_pq"
    val po_pq = sqlContext.read
      .option("mergeSchema","true")
      .parquet(po_sourceFile_pq)
    //Register temp table
    po_pq.registerTempTable("purchase_order_t")
    po_pq.printSchema()

    /***
     * GET LOYAL USERS
     */
    val po=sqlContext.sql(
      """select cosmos_customerid
                , header_purchase_date
                , purchase_channel
                , '36' as rolling_period
               from purchase_order_t po
      where months_between(from_unixtime(unix_timestamp()),header_purchase_date) < 36.0
      union
      select cosmos_customerid
                , header_purchase_date
                , purchase_channel
                , '24' as rolling_period
                from purchase_order_t po
      where months_between(from_unixtime(unix_timestamp()),header_purchase_date) < 24.0
      union
      select cosmos_customerid
                , header_purchase_date
                , purchase_channel
                , '12' as rolling_period
                from purchase_order_t po
      where months_between(from_unixtime(unix_timestamp()),header_purchase_date) < 12.0
     """)
    po.show()
    //writeParquet(po,"rolling_po")
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
      .save(root+fileName)
    }


}

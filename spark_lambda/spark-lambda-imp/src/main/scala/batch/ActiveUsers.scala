package batch

import java.lang.management.ManagementFactory

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ActiveUsers {
  def main(args: Array[String]): Unit = {
    // get spark configuration

    val conf = new SparkConf()
      .setAppName("Lambda with Spark")

    // Check if running from IDE
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "c:\\WinUtils") // required for winutils
      conf.setMaster("local[*]")
    }

    // setup spark context
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // initialize input RDD
    val sourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\purchase_order.tsv"
    val input = sc.textFile(sourceFile)

    //val parquetDF=input.toDF()
    //writeParquet(parquetDF,"purchase_order_parquet")
    input.foreach(println)

    //val inputDF = input.map { line =>
    //flatMap expects a type that would unbox
    val inputDF = input.flatMap { line =>
      //split and assign it to a variable
      val record = line.split("\\t")
      //Milli second in hour. No of milli sec in hour. Used to convert the original time stamp to an hourly timestamp
     //Validate if the actual input is of the actual length
      println(record.length)
      if (record.length == 13){
        //flatMap would unbox and return the activity
        Some(domain.PurchaseOrder(record(0), record(1), record(2), record(3), record(4), record(5), record(6),record(7), record(8), record(9), record(10), record(11), record(12)))
      }
      else
        None
    }.toDF()

    val df =inputDF.select(
      //Function to add one month( Time_stamp_hour would be a month ahead.
      inputDF("order_number"),inputDF("cosmos_customerid"),inputDF("header_purchase_date")
    )
    // register temp table
    df.registerTempTable(tableName="purchase_order")

    val visitorsByProduct=sqlContext.sql(
      //triple quote is used for multiline code
      """SELECT
        *
        from purchase_order
       """).cache()
    visitorsByProduct.show()

  }
  /*
  Function to write parquet files
   */
  def writeParquet(dfName:DataFrame, fileName:String):Unit={
    dfName.write
      .option("header", "true")
      .mode("overwrite")
      .save("C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\"+fileName)
    }
}

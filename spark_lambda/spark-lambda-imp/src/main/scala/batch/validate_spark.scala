package batch

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object validate_spark {
  def main (args: Array[String]): Unit = {

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

    // initialize input RDD
    val sourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\purchase_order.tsv"
    val inputRDD = sc.textFile(sourceFile)
    //Check if we are receiving the data
    inputRDD.foreach(println)
    val split= inputRDD.flatMap {line =>
      //split and assign to a variable
      val record =line.split("\\t")
      if(record.length == 11) {
        Some(domain.PurchaseOrder(record(0), record(1), record(2), record(3), record(4), record(5), record(6), record(7), record(8), record(9), record(10), record(11)))
      }
    None
    }
}}

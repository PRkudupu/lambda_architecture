package batch
import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object BatchJob {
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
    val sourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\data.tsv"
    val input = sc.textFile(sourceFile)

    //val inputDF = input.map { line =>
    //flatMap expects a type that would unbox
     val inputRDD = input.flatMap { line =>
      //split and assign it to a variable
      val record = line.split("\\t")
      //Milli second in hour. No of milli sec in hour. Used to convert the original time stamp to an hourly timestamp
      val MS_IN_HOUR = 1000 * 60 * 60
      //Validate if the actual input is of the actual length
      if (record.length == 7){
        //flatMap would unbox and return the activity
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      }
      else
        None
      }
    //It is the key and the first item is tuple and second item is entire activity
    //Caching is done here
    val keyedByProduct =inputRDD.keyBy(a=> (a.product,a.timestamp_hour)).cache()
    //Aggregate visitors by product.
    val visitorsByProduct = keyedByProduct
      .mapValues(a =>a.visitor)
      .distinct()
      .countByKey()

    //Count the unique no of visits /per hour per product
    //Match is like a switch
    val activityByProduct =keyedByProduct
      //Multiline lambda has to be replaced from paranthesis to curly
      //Find the type of activity
      .mapValues{
        a => a.action match {
            //In case of purchase, return a tuple of 3 values(1,0,0)
          case "purchase" =>(1,0,0)
          case "add_to_cart" =>(0,1,0)
          case "page_view" =>(0,0,1)
        }
      }
      //sum the result
      .reduceByKey((a,b)=>(a._1 +b._1,a._2+b._2,a._3+a._3))
    //Shows every product,timestamp and no of unique visitors
    //((Orbit,Spearmint Sugarfree Gum,1567454400000),1)
    visitorsByProduct.foreach(println)
    //would have product, hour(key) and activity(purchase/add_to_target/page_view)
    //((Opti-Free,Pure Moist Contact Solution,1567454400000),(0,0,1))
    activityByProduct.foreach(println)
}
}

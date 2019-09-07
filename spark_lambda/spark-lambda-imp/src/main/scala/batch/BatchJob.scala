package batch
import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object BatchJob {
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
    val sourceFile = "C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\data.tsv"
    val input = sc.textFile(sourceFile)

    //val inputDF = input.map { line =>
    //flatMap expects a type that would unbox
      val inputDF = input.flatMap { line =>
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
    }.toDF()
    val df =inputDF.select(
      //Function to add one month( Time_stamp_hour would be a month ahead.
      add_months(from_unixtime(inputDF("timestamp_hour")/1000),numMonths =1).as(alias="timestamp_hour")
      ,inputDF("referrer"),inputDF("action"),inputDF("prevPage"),inputDF("page"),inputDF("visitor"),inputDF("product")
    )
    // register temp table
    df.registerTempTable(tableName="activity")

    val visitorsByProduct=sqlContext.sql(
      //triple quote is used for multiline code
      """SELECT
       product,
        timestamp_hour,
        sum(case when action ='purchase' then 1 else 0 end) as purchase_count,
        sum(case when action ='add_to_cart' then 1 else 0 end) as add_to_cart_count,
        sum(case when action ='page_view' then 1 else 0 end) as page_view_count
        from activity
        group by product, timestamp_hour
       """).cache()
    visitorsByProduct.show()
    //Shows every product,timestamp and no of unique visitors
    //((Orbit,Spearmint Sugarfree Gum,1567454400000),1)
    visitorsByProduct.write
      .option("header", "true")
      .mode("overwrite")
      .save("C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\output.csv")


  }
}

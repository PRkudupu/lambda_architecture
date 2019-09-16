package batch

import java.lang.management.ManagementFactory

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object LoyalUsers {
  val root ="C:\\Boxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\data\\"
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(init())
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    /****
     * CUSTOMER LOYALTY DETAIL
     * **********/
     /**
     Read text file from the specified location.
     Add schema to the file
     */
    val sourceFile =root+"customer_loyalty_details.tsv"
    val input = sc.textFile(sourceFile)

    val inputDF = input.flatMap { line =>
      //split and assign it to a variable
      val record = line.split("\\t")
      //Validate if the actual input is of the actual length
      println(record.length)
      if (record.length == 13){
        //flatMap would unbox and return the activity
        Some(domain.CustomerLoyaltyDetails(record(0), record(1), record(2), record(3), record(4), record(5), record(6),record(7),
          record(8), record(9), record(10), record(11), record(12)))
      }
      else
        None
    }.toDF()
    inputDF.show()
    /**
     * Write dataframe with schema as parquet file
     */
    writeParquet(inputDF,"customer_loyalty_details_pq")

    val parquetSourceFile = root+"customer_loyalty_details_pq"
    val parquetFile = sqlContext.read
      .option("mergeSchema","true")
      .parquet(parquetSourceFile)
    //Register temp table
    parquetFile.registerTempTable("customer_loyalty_details_t")
    parquetFile.printSchema()

    /**
     * GET ACTIVE USERS FOR THE LAST 36, 24 AND 12 MONTHS
     */
    val customers=sqlContext.sql("""select * from customer_loyalty_details_t""")
    customers.foreach(println)

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
    writeParquet(po_inputDF,"purchase_order_parquet")

    /**
     * Read for parquet file
     */

    val po_sourceFile_pq = root+"purchase_order_parquet"
    val po_pq = sqlContext.read
      .option("mergeSchema","true")
      .parquet(po_sourceFile_pq)
    //Register temp table
    po_pq.registerTempTable("purchase_order_t")
    po_pq.printSchema()

    /***
     * GET LOYAL USERS
     */
    val loyalCustomers=sqlContext.sql("""select po.cosmos_customerid
       , po.purchase_channel
       , lo.loyalty_signup_date
       , lo.is_24_months
       , lo.is_12_months
       , po.header_purchase_date
        from purchase_order_t po
       join

        (select cosmos_customerid
               , loyalty_signup_date
               , case when months_between(from_unixtime(unix_timestamp()),loyalty_signup_date) < 24.0
                        then 1 else 0 end as is_24_months
               , case when months_between(from_unixtime(unix_timestamp()),loyalty_signup_date) < 12.0
                        then 1 else 0 end as is_12_months
        from customer_loyalty_details_t
        where months_between(from_unixtime(unix_timestamp()),
                loyalty_signup_date) < 36.0
                ) lo
        on po.cosmos_customerid=lo.cosmos_customerid""")
    loyalCustomers.foreach(println)

    /**
    GET THE LAST TRANSACTION FOR BOTH ONLINE AND STORE

     */
    loyalCustomers.registerTempTable("last_transaction_t")

     val lastTransactions_df=sqlContext.sql("""
                                           select cosmos_customerid,
                                           max(header_purchase_date) as max_date
                                              , 'ONLINE' as primary_channel
                          from last_transaction_t
                       where purchase_channel='ONLINE'
                       group by cosmos_customerid
                     union all
                     select cosmos_customerid,max(header_purchase_date) as max_date
                        , 'STORE' as primary_channel
                         from last_transaction_t
                       where purchase_channel='STORE'
                       group by cosmos_customerid
                       """)
    lastTransactions_df.foreach(println)

    lastTransactions_df.registerTempTable("customer_behaviour_t")
    val custBehaviour=sqlContext.sql("""select distinct(b.cosmos_customerid)
                                    ,case when is_omni='OMNI' then 'OMNI' else b.primary_channel end
                                  from customer_behaviour_t b
                              join
                                  (select cosmos_customerid
                                  , case when count(cosmos_customerid)  > 1 then 'OMNI' end as is_omni
                                  from customer_behaviour_t
                                   group by cosmos_customerid) a
                     on b.cosmos_customerid=a.cosmos_customerid """)
    println("final table")
    custBehaviour.foreach(println)

    /**
     * write output as parquet
     */
    writeParquet(custBehaviour,"crm_signal_file_pq")
    val crmsignal_source_pq = root+"crm_signal_file_pq"
    val crmsignal_pq = sqlContext.read
      .option("mergeSchema","true")
      .parquet(crmsignal_source_pq)
    crmsignal_pq.show()

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

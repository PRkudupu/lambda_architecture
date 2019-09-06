package object domain {
  case class Activity(timestamp_hour:Long,
                      referrer:String,
                      action:String,
                      prevPage:String,
                      visitor:String,
                      page:String,
                      product:String,
                      inputProps:Map[String,String]=Map()
                     )
  case class PurchaseOrder(order_number	:String,
                      storeid	:String,
                      purchase_channel:String,
                      header_purchase_date	:String,
                      order_date	:String,
                      total_net_amount	:String,
                      total_gross_amount	:String,
                      total_gross_units:String,
                      total_returned_units:String,
                      total_discount_amount:String,
                      total_return_sales:String,
                      dt1:String,
                      inputProps:Map[String,String]=Map()
                     )

}

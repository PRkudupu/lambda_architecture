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
                           cosmos_customerid:String,
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
                            dt:String,
                            inputProps:Map[String,String]=Map()
                     )
  case class CustomerIdentity(cosmos_customerid	:String,
                              alternateid:String,
                              alternateid_type	:String,
                              customer_sourceid:String,
                              channel_name	:String,
                              dt	:String,
                              inputProps:Map[String,String]=Map()
                          )
  case class CustomerLoyaltyDetails(cosmos_customerid	:String,
                                    loyalty_card_number:String,
                                    loyalty_registration_date:String,
                                    registration_event	:String,
                                    loyalty_signup_channel	:String,
                                    loyalty_signup_date	:String,
                                    customer_identifier:String,
                                    registration_summary	:String,
                                    loyalty_lifetime_points:String,
                                    loyalty_current_points	:String,
                                    loyalty_signup_storeid	:String,
                                    loyalty_status_rfval_id:String,
                                    dt:String,
                                    inputProps:Map[String,String]=Map()
                             )

}

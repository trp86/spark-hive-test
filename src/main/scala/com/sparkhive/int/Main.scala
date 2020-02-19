package com.sparkhive.int

import com.sparkhive.int.commons.LibCommons
import org.apache.spark.sql.SparkSession

object Main extends LocalConf {



  def main(args: Array[String]): Unit = {

    //Create dataframes for hive tables
    val leftTable=LibCommons.createDataFrameFromHiveTable("cust_details.customer")
    val rightTable=LibCommons.createDataFrameFromHiveTable("cust_details.customer_booking")

    //Join dataframes
    val joinDF=LibCommons.joinTables(leftTable,rightTable,Seq("customer_uuid"),"inner")

    //Store dataframe to a hive table
    LibCommons.storeDataFrameToHiveTable(joinDF,"cust_details.customer_booking_info")
  }


}

package com.sparkhive.int

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait LocalConf{
  //def appName:String=""
 // def mode:String="local"
  //val conf =new SparkConf().setAppName(appName).setMaster(mode)
  //val sparkContext =new SparkContext(conf)

  // Creation of SparkSession
  def sparkSession: SparkSession=SparkSession.builder()
    .appName("example-spark-scala-read-and-write-from-hive")
    // .config("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()


/*  SparkSession.builder()
    .appName("example-spark-scala-read-and-write-from-hive")
    // .config("hive.metastore.warehouse.dir", params.hiveHost + "user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()*/

}

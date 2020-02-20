package com.sparkhive.int

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import com.sparkhive.int.commons.{HiveTableNotFound, JoinTypeNotFound, LibCommons}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class SparkHiveIntTest extends
  FunSpec
  with BeforeAndAfterEach
  with DatasetSuiteBase
  with LocalConf
  with DataFrameSuiteBase
{

  override def conf= super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
  val resourcePath="src/test/resources/"
  override def sparkSession: SparkSession = spark

  override def beforeEach() {

    //DROP IF EXISTS
    sparkSession.sql("DROP TABLE IF EXISTS cust_details.customer")
    sparkSession.sql("DROP TABLE IF EXISTS cust_details.customer_booking")
    sparkSession.sql("DROP DATABASE IF EXISTS cust_details")

    //Create Customer database
    sparkSession.sql("DROP DATABASE IF EXISTS cust_details")
    sparkSession.sql("CREATE DATABASE IF NOT EXISTS cust_details LOCATION '/tmp/cust_details.db'")

    //Create customer table

    //Schema
   /* val customerSchema=StructType(Array(
      StructField("customer_uuid",StringType,nullable = false),
      StructField("email_hash",StringType,nullable = false),
      StructField("customer_type",StringType,nullable = false),
      StructField("preferred_lan_code",StringType,nullable = false)
    ))*/

    //Dataframe
    SparkHiveIntTest.df_customer=sparkSession.read/*.schema(customerSchema)*/.option("header","true").option("inferSchema","true").csv(resourcePath+"customer.csv")
    //Saving the datarame
    SparkHiveIntTest.df_customer.write.saveAsTable("cust_details.customer")


    //Create the customer_booking

    //Schema
   /* val customerBookingSchema=StructType(Array(
      StructField("customer_uuid",StringType,nullable = false),
      StructField("booking_type",StringType,nullable = false),
      StructField("booking_code",StringType,nullable = false),
      StructField("booking_amount",DoubleType,nullable = false)
    ))*/

    //Dataframe
    SparkHiveIntTest.df_customerBooking=sparkSession.read.option("header","true").option("inferSchema","true").csv(resourcePath+"customer_booking.csv")
    //Saving the datarame
    SparkHiveIntTest.df_customerBooking.write.saveAsTable("cust_details.customer_booking")


    //Create the customer_booking_info
    SparkHiveIntTest.df_customer_booking_info=sparkSession.read.option("header","true").option("inferSchema","true").csv(resourcePath+"customer_booking_info.csv")



  }

  //LibCommons.createDataFrameFromHiveTable
  describe("Test-createDataFrameFromHiveTable")
  {
    it("should create a dataframe when we pass a valid hive table name")
    {
     val df: DataFrame = LibCommons.createDataFrameFromHiveTable("cust_details.customer")
      assertDataFrameEquals(df,SparkHiveIntTest.df_customer)
    }

    it("should throw exception when passed an hive table name which doesnot exist")
    {
      val exception=intercept[AnalysisException]
      {
        LibCommons.createDataFrameFromHiveTable("cust_details.customer_history")
      }
    }

    it("should throw exception when passed null")
    {
      val exception=intercept[HiveTableNotFound]
        {
          LibCommons.createDataFrameFromHiveTable(null:String)
        }
      assert(exception.getMessage=="Hive table name cannot be null.")
    }

    it("should throw exception when passed string length<=0")
    {
      val exception=intercept[HiveTableNotFound]
        {
          LibCommons.createDataFrameFromHiveTable("")
        }
      assert(exception.getMessage=="Hive table name length <= 0.")
    }
  }

  //LibCommons.joinTables
  describe("Test-joinTables")
  {
    it("should perform inner join and return dataframe")
    {
      val join_df=LibCommons.joinTables(SparkHiveIntTest.df_customer,
        SparkHiveIntTest.df_customerBooking,
        Seq("customer_uuid"),
        "inner")

      //assertDataFrameEquals function fails when the order of the columns are different
      //Below workaround reorders the columns of the dataframe before doing comparison
      val columns = join_df.columns
      assertDataFrameEquals(SparkHiveIntTest.df_customer_booking_info.select(columns.head,columns.tail:_*),
        join_df.select(columns.head,columns.tail:_*))
    }

    it("should generate exception when passed invalid join column name") {
      val exception=intercept[AnalysisException]
      {
        val join_df=LibCommons.joinTables(SparkHiveIntTest.df_customer,
          SparkHiveIntTest.df_customerBooking,
          Seq("1customer_uuid"),
          "inner")
      }
    }

    it("should generate exception when join type is other than [inner]") {
      val exception=intercept[JoinTypeNotFound]
        {
          val join_df=LibCommons.joinTables(SparkHiveIntTest.df_customer,
            SparkHiveIntTest.df_customerBooking,
            Seq("customer_uuid"),
            "1inner")
        }
      assert(exception.getMessage=="Join type [1inner] not found.")
    }

  }

  override def afterAll() {
    super.afterAll()

    //Drop tables
    spark.sql("DROP TABLE IF EXISTS cust_details.customer")
    spark.sql("DROP TABLE IF EXISTS cust_details.customer_booking")
    spark.sql("DROP DATABASE IF EXISTS cust_details")
  }


}

object SparkHiveIntTest {
  var df_customer:DataFrame=_
  var df_customerBooking:DataFrame=_
  var df_customer_booking_info:DataFrame=_
}

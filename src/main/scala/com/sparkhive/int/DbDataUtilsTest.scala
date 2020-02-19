package com.sparkhive.int


import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetGenerator, RDDGenerator, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import com.holdenkarau.spark.testing.DatasetSuiteBase

class HiveTest extends FunSuite  with DatasetSuiteBase{
  //override def conf: LocalConfurigation = super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
    //set("spark.sql.hive.metastore.jars","maven")

  import spark.implicits._

  test("hive support") {
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db11 LOCATION '/tmp/test-db11.db'")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db11.tbl1 (col1 string)")
  }
}

class DbDataUtilsTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase with Matchers
{
  override implicit def reuseContextIfPossible: Boolean = true
  //override def conf: LocalConfurigation = super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
  import spark.implicits._

  test("TEST")
  {

   /*val conf = new SparkConf()
    .set("spark.sql.warehouse.dir", "/tmp/")
      .set("spark.sql.catalogImplementation","hive")
      .setMaster("local[*]")
      .setAppName("Hive Example")*/

    /*val spark1 = SparkSession.builder()
     /* .config(conf)*/
      .enableHiveSupport()
      .getOrCreate()
*/

    spark.sql("CREATE DATABASE IF NOT EXISTS test_db1 LOCATION '/tmp/test-db1.db'")
    spark.sql("CREATE TABLE IF NOT EXISTS test_db1.tbl1 (col1 string)")
    //spark.sql("select count(*) from test_db1.tbl1")
  }

}

class Tests extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _

  override def beforeEach() {

    sparkSession = SparkSession.builder().appName("udf testings")

      .master("local")

      .config("", "")

      .getOrCreate()

  }

  test("your test name here"){

    //your unit test assert here like below

    assert("True".toLowerCase == "true")

  }

  override def afterEach() {

    sparkSession.stop()

  }

}

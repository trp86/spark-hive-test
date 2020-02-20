package com.sparkhive.int.commons

import com.sparkhive.int.LocalConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession,AnalysisException}

object LibCommons extends LocalConf{

  def joinTables(leftTable:DataFrame,rightTable:DataFrame,columnNames:Seq[String],joinType:String):DataFrame=
  {
    try
      {
        if (joinType.toLowerCase=="inner")
        {
          leftTable.join(rightTable,columnNames,joinType.trim.toLowerCase)
        }
        else
        {
          throw JoinTypeNotFound("Join type ["+joinType+"] not found.")
        }
      }
    catch
      {
        case joinTypeNotFound: JoinTypeNotFound=>throw joinTypeNotFound
        case analysisException: AnalysisException=>throw analysisException
        case exception: Exception=>throw exception
      }

  }


  def createDataFrameFromHiveTable(hiveTableName:String):DataFrame=
    {
      try
        {
          //print("HIVE TABLE:---"+hiveTableName)
          if (hiveTableName eq null) throw new HiveTableNotFound("Hive table name cannot be null.")
          if (hiveTableName.trim.length<=0) throw new HiveTableNotFound("Hive table name length <= 0.")
          sparkSession.sql("select * from "+hiveTableName.trim)
        }
      catch
        {
          case hiveTableNotFound:HiveTableNotFound=>throw hiveTableNotFound
          case analysisException:AnalysisException=>throw analysisException
          case e:Exception=>throw e
        }

    }

  def storeDataFrameToHiveTable(df:DataFrame,hiveTableName:String):Boolean=
  {
    df.write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName.trim)
    true
  }


}

//Custom Exceptions
case class HiveTableNotFound(s: String)  extends Exception(s)
case class JoinTypeNotFound(s: String)  extends Exception(s)
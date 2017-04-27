package com.evergrande.bigdata.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Yu on 16/7/6.
  */
object HiveUtils {

  var sqlContext: HiveContext = null

  def getDataFromHive(sqlStmt: String, sc: SparkContext, numPartitions: Int): DataFrame = {

    if (sqlContext == null) {
      sqlContext = new HiveContext(sc)
    }
    sqlContext.sql("use csum")

    val dataFrame = sqlContext.sql(sqlStmt).coalesce(numPartitions)

    if (TransformerConfigure.isDebug) {
      println("Sample result of the SQL [%s]:".format(sqlStmt))
      dataFrame.show(100)

//      println("%d rows returned from Hive.".format(dataFrame.count()))
    }

    dataFrame
  }

}

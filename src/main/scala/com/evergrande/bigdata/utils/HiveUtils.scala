package com.evergrande.bigdata.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Yu on 16/7/6.
  */
object HiveUtils {

  var sqlContext: HiveContext = null

  def getDataFromHive(sqlStmt: String, sc: SparkContext): DataFrame = {

    if (sqlContext == null) {
      sqlContext = new HiveContext(sc)
    }
    sqlContext.sql("use csum")

    val dataFrame = sqlContext.sql(sqlStmt).coalesce(64).cache()

    if (TransformerConfigure.isDebug) {
      println("Sample Data: ")
      dataFrame.show(10)

      println("Number of rows returned : " + dataFrame.count())
    }

    dataFrame
  }

}

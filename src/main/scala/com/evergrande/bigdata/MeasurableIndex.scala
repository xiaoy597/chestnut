package com.evergrande.bigdata

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.Row

/**
  * Created by Yu on 16/7/6.
  */
case class MeasurableIndex(indexMap: Map[String, BigDecimal], today: Date, tableConfig: FlatTableConfig) {
  type RowWithRuleType = (Row, List[(String, String)])

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //  var today = dateFormat.parse("2016-05-10")

  def getDateBeforeDays(nDays: Int): Date = {
    val rightNow = Calendar.getInstance()

    rightNow.setTime(today)

    rightNow.add(Calendar.DAY_OF_YEAR, -nDays)

    rightNow.getTime
  }

  val date7DaysBefore = getDateBeforeDays(7)
  val date30DaysBefore = getDateBeforeDays(30)
  val date10DaysBefore = getDateBeforeDays(10)
  val date15DaysBefore = getDateBeforeDays(15)

  def create(rowWithRule: RowWithRuleType): MeasurableIndex = {

    var indexMap = Map[String, BigDecimal]()

    for (indexRule <- rowWithRule._2) {
      indexMap = updateIndexValue(rowWithRule._1, indexRule, indexMap)
    }

    MeasurableIndex(indexMap, today, tableConfig)
  }

  def augment(rowWithRule: RowWithRuleType): MeasurableIndex = {
    var indexMap = this.indexMap

    for (indexRule <- rowWithRule._2) {
      indexMap = updateIndexValue(rowWithRule._1, indexRule, indexMap)
    }

    MeasurableIndex(indexMap, today, tableConfig)
  }

  def merge(otherResult: MeasurableIndex): MeasurableIndex = {
    MeasurableIndex(indexMap ++ otherResult.indexMap, today, tableConfig)
  }

  def updateIndexValue(row: Row,
                       indexRule: (String, String),
                       indexMap: Map[String, BigDecimal]
                      ): Map[String, BigDecimal] = {
    val indexDate = dateFormat.parse(row.getAs[String]("statt_dt"))
    val indexCalcMode = indexRule._2

    val targetIndexName = "metric" + "." +
      row.getAs[String](tableConfig.dim_clmn_nm).toLowerCase + "." +
      row.getAs[String]("statt_indx_id").toLowerCase + "." +
      indexRule._1 + "." + (
      indexCalcMode match {
        case "10" => "current"
        case "20" => "sum7d"
        case "21" => "sum30d"
        case "22" => "sum10d"
        case "23" => "sum15d"
        case _ => "unknown"
      })

    val indexValue : BigDecimal = {
      val v = row.getDecimal(row.fieldIndex(indexRule._1))
      if (v == null) 0.0 else v
    }

    indexCalcMode match {
      case "10" if indexDate.equals(today) => // Directly assignment for indices of current date.
        indexMap + (targetIndexName -> indexValue)
      case "20" if indexDate.after(date7DaysBefore) => // Aggregate on last 7 days.
        if (indexMap.contains(targetIndexName))
          indexMap + (targetIndexName -> (indexValue + indexMap(targetIndexName)))
        else
          indexMap + (targetIndexName -> indexValue)
      case "21" if indexDate.after(date30DaysBefore) => // Aggregate on last 30 days.
        if (indexMap.contains(targetIndexName))
          indexMap + (targetIndexName -> (indexValue + indexMap(targetIndexName)))
        else
          indexMap + (targetIndexName -> indexValue)
      case "22" if indexDate.after(date10DaysBefore) => // Aggregate on last 30 days.
        if (indexMap.contains(targetIndexName))
          indexMap + (targetIndexName -> (indexValue + indexMap(targetIndexName)))
        else
          indexMap + (targetIndexName -> indexValue)
      case "23" if indexDate.after(date15DaysBefore) => // Aggregate on last 30 days.
        if (indexMap.contains(targetIndexName))
          indexMap + (targetIndexName -> (indexValue + indexMap(targetIndexName)))
        else
          indexMap + (targetIndexName -> indexValue)
      case _ => indexMap
    }

  }
}

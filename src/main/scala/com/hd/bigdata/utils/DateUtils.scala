package com.hd.bigdata.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by Yu on 16/5/10.
  */
object DateUtils {
  var today : Date = new Date()

  def getDateBeforeDays(nDays: Int): Date = {
    val rightNow = Calendar.getInstance()

    rightNow.setTime(today)

    rightNow.add(Calendar.DAY_OF_YEAR, -nDays)

    rightNow.getTime
  }

}

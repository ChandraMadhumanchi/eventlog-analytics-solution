package com.user.spark

import java.sql.Timestamp
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Date

object StringImplicits {
 
  
  def getDateFromStr(timeStr: String, format: String): Date = {
    val dateFormat: DateFormat = new SimpleDateFormat(format)

    val date: Option[Date] = {
      try {
        Some(dateFormat.parse(timeStr))
      } catch {
        case e: java.text.ParseException => None
      }
    }
    date.getOrElse(null)
  }
  
}
package com.inforefiner

object Yesterday {

  def main(args: Array[String]): Unit = {

    var calender = java.util.Calendar.getInstance()
    calender.set(java.util.Calendar.DAY_OF_YEAR, calender.get(java.util.Calendar.DAY_OF_YEAR) - 1)
    calender.set(java.util.Calendar.HOUR_OF_DAY, 0)
    var sdf = new java.text.SimpleDateFormat("yyyyMMdd")
    var date = sdf.format(calender.getTime)
    var prefix = "/tmp/zhaojindong/stag."
    println(prefix + date)

  }

}

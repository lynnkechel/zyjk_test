package com.inforefiner

object BeforeOneHour {

  def main(args: Array[String]): Unit = {

    var calender = java.util.Calendar.getInstance()
    calender.set(java.util.Calendar.HOUR_OF_DAY, calender.get(java.util.Calendar.HOUR_OF_DAY) - 1)
    var sdf = new java.text.SimpleDateFormat("yyyyMMddHH")
    var beforeOneHour = sdf.format(calender.getTime)
    println(beforeOneHour)

  }

}

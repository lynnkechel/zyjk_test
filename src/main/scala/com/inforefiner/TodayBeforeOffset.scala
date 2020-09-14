package com.inforefiner

object TodayBeforeOffset {

  def main(args: Array[String]): Unit = {

    var prefix = "/tmp/zjd/ALL_ORD/"
    var offset = 15
    var sep = ","
    var dateFormat = "yyyy-MM-dd-HH"

    var calender = java.util.Calendar.getInstance()
    calender.add(java.util.Calendar.DATE, -offset)
    calender.set(java.util.Calendar.HOUR_OF_DAY, 0)
    var sdf = new java.text.SimpleDateFormat(dateFormat)
    var list = new java.util.ArrayList[String]()
    for(j <- 0 to (offset - 1)){
      for(l <- 0 to 23){
        list.add(prefix + sdf.format(calender.getTime()))
        calender.add(java.util.Calendar.HOUR_OF_DAY, 1)
      }
  }
    println(java.lang.String.join(sep, list))

  }

}

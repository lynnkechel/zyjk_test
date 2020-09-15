package com.inforefiner

import java.util

object YesterdayAllHour {

  def main(args: Array[String]): Unit = {

    var calender = java.util.Calendar.getInstance()
    calender.set(java.util.Calendar.DAY_OF_YEAR, calender.get(java.util.Calendar.DAY_OF_YEAR) - 1)
    calender.set(java.util.Calendar.HOUR_OF_DAY, 0)
    var sdf = new java.text.SimpleDateFormat("yyyyMMddHH")
    var list = new java.util.ArrayList[String]()
    var prefix= "/data/zyjk/OF_SERV_FIN/STAG."
    for(j <- 0 to 23){
      list.add(prefix + sdf.format(calender.getTime()))
      calender.add(java.util.Calendar.HOUR_OF_DAY, 1)
    }
    val str = java.lang.String.join(",", list)
    println(str)

  }

}

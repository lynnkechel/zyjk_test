package com.inforefiner

object Last7WeekDaysHour {

  def main(args: Array[String]): Unit = {

    var calender = java.util.Calendar.getInstance()
    calender.add(java.util.Calendar.DATE, - (calender.get(java.util.Calendar.DAY_OF_WEEK) - calender.getFirstDayOfWeek()) - 7)
    calender.add(java.util.Calendar.DAY_OF_YEAR, 1)
    calender.add(java.util.Calendar.HOUR_OF_DAY, 0)
    var sdf = new java.text.SimpleDateFormat("yyyy-MM-dd-HH")
    var list = new java.util.ArrayList[String]()
    var prefix= "/data/zyjk/OF_SERV_FIN/"
    for(j <- 0 to 6){
      for(l <- 0 to 23){
        list.add(prefix + sdf.format(calender.getTime()))
        calender.add(java.util.Calendar.HOUR_OF_DAY, 1)
      }
    }
    println(java.lang.String.join(",", list))

  }


}

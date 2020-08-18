package com.inforefiner

object Last7WeekDays {

  def main(args: Array[String]): Unit = {

    var calender = java.util.Calendar.getInstance()
    calender.add(java.util.Calendar.DATE, - (calender.get(java.util.Calendar.DAY_OF_WEEK) - calender.getFirstDayOfWeek()) - 7)
    var sdf = new java.text.SimpleDateFormat("yyyy-MM-dd-HH")
    var list = new java.util.ArrayList[String]()
    var prefix= "/data/zyjk/OF_SERV_FIN/"
    for(j <- 1 to 7){
        calender.add(java.util.Calendar.DAY_OF_YEAR, 1)
        list.add(prefix + sdf.format(calender.getTime()))
    }
    println(java.lang.String.join(",", list))
  }

}

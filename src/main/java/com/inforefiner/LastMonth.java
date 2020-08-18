package com.inforefiner;

import java.util.Date;

public class LastMonth {

    public static void main(String[] args){

        int offset = 0;

        Date date = new Date();

        date.setMonth(date.getMonth() + offset);

        int str_year = date.getYear() + 1900;
        int str_month = date.getMonth() + 1;

        String month = "";

        if(str_month < 10){
            month = "0" + str_month;
        }

        System.out.println(str_year + "-" + month);

    }

}

package com.yyjr.kafka.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class DateUtil {

    public static void main(String[] args) throws ParseException {
        String[] enDate = new String[] {"Mon Dec 15 00:00:00 CST 2014", "27-NOV-17 12.04.51.000000 PM",
                "27-NOV-17 03.12.22.000000 PM", "18-DEC-17 12.52.21.000000 PM", "18-DEC-17 12.53.30.000000 PM",
                "18-DEC-17 03.35.24.000000 PM", "19-DEC-17 04.38.36.000000 PM", "30-NOV-17 06.54.32.000000 PM",
                "01-DEC-17 08.57.23.000000 AM", "01-DEC-17 08.54.45.000000 AM", "01-DEC-17 05.26.51.000000 PM"};
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMMM-yyyy HH.mm.ss.ms", Locale.US);

        java.util.Date fecha = new java.util.Date("Mon Dec 15 00:00:00 CST 2014");
        DateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
        Date date;
        date = (Date) formatter.parse(fecha.toString());
        System.out.println(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        String formatedDate =
                cal.get(Calendar.DATE) + "/" + (cal.get(Calendar.MONTH) + 1) + "/" + cal.get(Calendar.YEAR);
        System.out.println("formatedDate : " + formatedDate);

        for (String strDate : enDate) {
            System.out.print(strDate + "\t");
            System.out.println(dateFormat.format(strDate));
        }
    }
}

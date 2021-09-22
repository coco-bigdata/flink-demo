package com.github.zhangchunsheng.flink.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 日期格式化工具类
 */
public class DateTimeUtil {

    /**
     * 格式 yyyy年MM月dd日 HH:mm:ss
     *
     * @param dateTime
     * @return
     */
    public static String getDateTimeDisplayString(LocalDateTime dateTime) {
        DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyy年MM月dd日 HH:mm:ss");
        String strDate2 = dtf2.format(dateTime);

        return strDate2;
    }

    public static String getDateTimeDisplayString(Integer time) {
        Date date = new Date(Long.valueOf(time) * 1000);
        LocalDateTime ldt = date.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        return DateTimeUtil.getDateTimeDisplayString(ldt);
    }

    public static String getDateTimeString(LocalDateTime dateTime) {
        DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String strDate2 = dtf2.format(dateTime);

        return strDate2;
    }

    public static String getDateTimeString(Integer time) {
        Date date = new Date(Long.valueOf(time) * 1000);
        LocalDateTime ldt = date.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        return DateTimeUtil.getDateTimeString(ldt);
    }

    public static String format(Integer time) {
        if(time.equals(0)) {
            return "";
        }
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date(time.longValue() * 1000));
    }

    public static String getPresentDate(String format) {
        SimpleDateFormat df = null;

        Date date = new Date();

        df = new SimpleDateFormat(format);

        return df.format(date);
    }
}

package com.github.zhangchunsheng.flink.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DateUtil {
    private static final String DATE_FORMAT = "EEE MMM dd HH:mm:ss z yyyy";

    private final static Log logger = LogFactory.getLog(DateUtil.class);

    public static String format(Integer time) {
        if (time.equals(0)) {
            return "";
        }
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date(time.longValue() * 1000));
    }

    public static String format(Long time, String format) {
        if (time.equals(0L)) {
            return "";
        }
        SimpleDateFormat df = new SimpleDateFormat(format);
        return df.format(new Date(time));
    }

    public static int getDateGMT(String dateString) {
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
        TimeZone tz = TimeZone.getTimeZone("GMT+8");
        sdf.setTimeZone(tz);
        try {
            Date parse = sdf.parse(dateString);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            String preCallbackTime = simpleDateFormat.format(parse);
            return Integer.valueOf(preCallbackTime);
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return 0;
    }

    public static int getDate(String dateStr) {
        try {
            if (dateStr == null || dateStr.isEmpty()) {
                return 0;
            }
            //2017-08-16 15:22:47.0
            SimpleDateFormat df = null;
            if (dateStr.indexOf("-") > 0) {
                df = new SimpleDateFormat("yyyy-MM-dd");
            } else {
                df = new SimpleDateFormat(DATE_FORMAT);
            }

            Date date = df.parse(dateStr);

            df = new SimpleDateFormat("yyyyMMdd");

            return Integer.parseInt(df.format(date));
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return 0;
    }

    public static String getDate(String dateStr, String fromFormat, String toFormat) {
        try {
            if (dateStr == null || dateStr.isEmpty()) {
                return "";
            }
            SimpleDateFormat df = new SimpleDateFormat(fromFormat);

            Date date = df.parse(dateStr);

            df = new SimpleDateFormat(toFormat);

            return df.format(date);
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return "";
    }

    public static String addDay(String date, int amount) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Date sDate = sdf.parse(date);
            Calendar c = Calendar.getInstance();
            c.setTime(sDate);
            c.add(Calendar.DAY_OF_MONTH, amount);
            sDate = c.getTime();

            return sdf.format(sDate);
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return "";
    }

    public static String getDate() {
        SimpleDateFormat df = null;

        Date date = new Date();

        df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return df.format(date);
    }

    public static String getPresentDate(String format) {
        SimpleDateFormat df = null;

        Date date = new Date();

        df = new SimpleDateFormat(format);

        return df.format(date);
    }

    public static int getWeekOfDate(String dateStr, String formatStr) {
        try {
            SimpleDateFormat format = new SimpleDateFormat(formatStr);
            Date date = format.parse(dateStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
            if (w < 0)
                w = 0;

            return w;
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return -1;
    }

    public static String getWeek(String dateStr, String formatStr) {
        try {
            SimpleDateFormat format = new SimpleDateFormat(formatStr);
            Date date = format.parse(dateStr);
            SimpleDateFormat sdf = new SimpleDateFormat("EEEE");
            String week = sdf.format(date);

            return week;
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return "";
    }

    public static int getSeason(String dateStr, String formatStr) {
        try {
            SimpleDateFormat format = new SimpleDateFormat(formatStr);
            Date date = format.parse(dateStr);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int m = calendar.get(Calendar.MONTH) + 1;
            int season = 0;
            if (m >= 1 && m <= 3) {
                season = 1;
            }
            if (m >= 4 && m <= 6) {
                season = 2;
            }
            if (m >= 7 && m <= 9) {
                season = 3;
            }
            if (m >= 10 && m <= 12) {
                season = 4;
            }
            return season;
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return -1;
    }

    public static String getSeasonStr(String dateStr, String fromFormat) {
        String year = getDate(dateStr, fromFormat, "yyyy");
        int season = getSeason(dateStr, fromFormat);
        return year + "0" + season;
    }

    public static int getWeekOfYear(String dateStr, String formatStr, boolean monday) {
        try {
            SimpleDateFormat format = new SimpleDateFormat(formatStr);
            Date date = format.parse(dateStr);

            Calendar calendar = Calendar.getInstance();
            if (monday) {
                calendar.setFirstDayOfWeek(Calendar.MONDAY);
            }
            calendar.setTime(date);
            return calendar.get(Calendar.WEEK_OF_YEAR);
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * 获取当前日期是星期几<br>
     *
     * @param dt Date
     * @return 当前日期是星期几
     */
    public static String getWeekOfDate(Date dt) {
        String[] weekDays = {"星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"};
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0)
            w = 0;
        return weekDays[w];
    }

    //根据日期取得星期几
    public static String getWeek(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("EEEE");
        String week = sdf.format(date);
        return week;
    }

    //取得日期是某年的第几周
    public static int getWeekOfYear(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
        return weekOfYear;
    }

    //取得某个月有多少天
    public static int getDaysOfMonth(int year, int month) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        int daysOfMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        return daysOfMonth;
    }

    // 取得两个日期之间的相差多少天
    public static long getDaysBetween(Date date0, Date date1) {
        long daysBetween = (date0.getTime() - date1.getTime() + 1000000) /
                86400000;// 86400000=3600*24*1000  用立即数，减少乘法计算的开销

        return daysBetween;
    }

    public static void main(String[] args) throws Exception {
        String date1 = getPresentDate("yyyyMMdd");
        SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
        Date parse = yyyyMMdd.parse("20210423");
        Date parse1 = yyyyMMdd.parse(date1);
        long daysBetween = getDaysBetween(parse1, parse);
        System.out.println(daysBetween);
    }

    public static int getTime(String dateStr) {
        try {
            if (dateStr == null || dateStr.isEmpty()) {
                return 0;
            }
            //2017-08-16 15:22:47.0
            SimpleDateFormat df = null;
            if (dateStr.indexOf("-") > 0) {
                df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            } else {
                df = new SimpleDateFormat(DATE_FORMAT);
            }

            Date date = df.parse(dateStr);

            return getSecondTimestamp(date);
        } catch (ParseException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取精确到秒的时间戳
     *
     * @return int
     */
    public static int getSecondTimestamp(Date date) {
        if (null == date) {
            return 0;
        }
        String timestamp = String.valueOf(date.getTime());
        int length = timestamp.length();
        if (length > 3) {
            try {
                return Integer.valueOf(timestamp.substring(0, length - 3));
            } catch (NumberFormatException e) {
                logger.error(e);
            }
            return 0;
        } else {
            return 0;
        }
    }

    /**
     * 获取今天零时的时间戳
     *
     * @return
     */
    public static long getTodayTimestamp() {
        long now = System.currentTimeMillis() / 1000l;
        long daysecond = 60 * 60 * 24;
        long daytime = now - (now + 8 * 3600) % daysecond;
        return daytime;
    }

    public static String getDay() {
        SimpleDateFormat df = null;

        Date date = new Date();

        df = new SimpleDateFormat("yyyyMMdd");

        return df.format(date);
    }

    public static String getTime() {
        SimpleDateFormat df = null;

        Date date = new Date();

        df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return df.format(date);
    }
}

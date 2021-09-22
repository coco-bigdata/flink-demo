package com.github.zhangchunsheng;

import com.github.zhangchunsheng.flink.utils.DateUtil;

public class Test {
    public static void main(String[] args) throws Exception {
        String day = DateUtil.getDay();
        System.out.println(day);

        Integer duration = 3441;
        double durationMinute = 3441.0 / 1000 / 60;
        durationMinute = duration.doubleValue() / 1000 / 60;
        System.out.println(durationMinute);
    }
}

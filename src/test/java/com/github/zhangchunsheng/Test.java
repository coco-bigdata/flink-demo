package com.github.zhangchunsheng;

import com.github.zhangchunsheng.flink.utils.DateUtil;

public class Test {
    public static void main(String[] args) throws Exception {
        String day = DateUtil.getDay();
        System.out.println(day);
    }
}

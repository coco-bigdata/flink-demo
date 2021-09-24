package com.github.zhangchunsheng.flink.pomegranate;

public class PaidWarn {
    private Long paidCreateTime;
    private boolean flag;

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public PaidWarn(boolean reallydData, int i, long l, String orderId) {

    }

    public Long getPaidCreateTime() {
        return paidCreateTime;
    }

    public void setPaidCreateTime(Long paidCreateTime) {
        this.paidCreateTime = paidCreateTime;
    }
}

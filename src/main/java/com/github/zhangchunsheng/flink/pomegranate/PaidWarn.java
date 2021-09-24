package com.github.zhangchunsheng.flink.pomegranate;

public class PaidWarn {
    private Long paidCreateTime;
    private boolean flag;
    private Integer key;
    private String orderId;
    private Long systemTimestamp;

    public Long getSystemTimestamp() {
        return systemTimestamp;
    }

    public void setSystemTimestamp(Long systemTimestamp) {
        this.systemTimestamp = systemTimestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public String getNumOrTimestamp() {
        return numOrTimestamp;
    }

    public void setNumOrTimestamp(String numOrTimestamp) {
        this.numOrTimestamp = numOrTimestamp;
    }

    private String numOrTimestamp;

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

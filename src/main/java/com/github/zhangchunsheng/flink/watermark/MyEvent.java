package com.github.zhangchunsheng.flink.watermark;

public class MyEvent {
    private Long creationTime;

    public Long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Long creationTime) {
        this.creationTime = creationTime;
    }
}

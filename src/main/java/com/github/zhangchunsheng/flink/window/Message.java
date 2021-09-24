package com.github.zhangchunsheng.flink.window;

import java.text.SimpleDateFormat;

public class Message {
    long timestamp;
    String value;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Message(long timestamp, String value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public String toString() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(this.timestamp) + " " + this.value;
    }
}

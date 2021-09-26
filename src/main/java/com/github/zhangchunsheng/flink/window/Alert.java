package com.github.zhangchunsheng.flink.window;

public class Alert {
    private String id;
    private String type;
    private String desc;

    public Alert(String id, String type) {
        this.id = id;
        this.type = type;
    }

    public Alert(String id, String type, String desc) {
        this.id = id;
        this.type = type;
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}

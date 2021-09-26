package com.github.zhangchunsheng.flink.model;

import com.google.gson.annotations.SerializedName;

public class CUnpackDataT {
    // timestamp statusDuration lastTimestamp
    @SerializedName("package_date")
    private Integer packageDate;

    @SerializedName("package_time")
    private Long packageTime;

    @SerializedName("status")
    private Integer status;

    @SerializedName("equipment_number")
    private String equipmentNumber;

    @SerializedName("ip")
    private String ip;

    @SerializedName("package_no")
    private Integer packageNo;

    @SerializedName("work_time")
    private Long workTime;

    @SerializedName("standby_time")
    private Long standbyTime;

    @SerializedName("warning_time")
    private Long warningTime;

    @SerializedName("piece_cnt")
    private Integer pieceCnt;

    public Long getPackageTime() {
        return packageTime;
    }

    public void setPackageTime(Long packageTime) {
        this.packageTime = packageTime;
    }

    public Integer getPackageDate() {
        return packageDate;
    }

    public void setPackageDate(Integer packageDate) {
        this.packageDate = packageDate;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getEquipmentNumber() {
        return equipmentNumber;
    }

    public void setEquipmentNumber(String equipmentNumber) {
        this.equipmentNumber = equipmentNumber;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPackageNo() {
        return packageNo;
    }

    public void setPackageNo(Integer packageNo) {
        this.packageNo = packageNo;
    }

    public Long getWorkTime() {
        return workTime;
    }

    public void setWorkTime(Long workTime) {
        this.workTime = workTime;
    }

    public Long getStandbyTime() {
        return standbyTime;
    }

    public void setStandbyTime(Long standbyTime) {
        this.standbyTime = standbyTime;
    }

    public Long getWarningTime() {
        return warningTime;
    }

    public void setWarningTime(Long warningTime) {
        this.warningTime = warningTime;
    }

    public Integer getPieceCnt() {
        return pieceCnt;
    }

    public void setPieceCnt(Integer pieceCnt) {
        this.pieceCnt = pieceCnt;
    }
}

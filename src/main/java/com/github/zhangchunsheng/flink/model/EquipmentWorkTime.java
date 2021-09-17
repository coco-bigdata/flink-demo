package com.github.zhangchunsheng.flink.model;

public class EquipmentWorkTime {
    // timestamp statusDuration lastTimestamp
    private Integer packageDate;
    private Long startPackageTime;
    private Long endPackageTime;
    private Integer status;
    private String equipmentNumber;

    private Integer statusDuration;
    private Double durationMinute;
    private String ip;
    private Integer packageNo;
    private Long workTime;

    private Long standbyTime;
    private Long warningTime;
    private Integer pieceCnt;

    public Integer getPackageDate() {
        return packageDate;
    }

    public void setPackageDate(Integer packageDate) {
        this.packageDate = packageDate;
    }

    public Long getStartPackageTime() {
        return startPackageTime;
    }

    public void setStartPackageTime(Long startPackageTime) {
        this.startPackageTime = startPackageTime;
    }

    public Long getEndPackageTime() {
        return endPackageTime;
    }

    public void setEndPackageTime(Long endPackageTime) {
        this.endPackageTime = endPackageTime;
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

    public Integer getStatusDuration() {
        return statusDuration;
    }

    public void setStatusDuration(Integer statusDuration) {
        this.statusDuration = statusDuration;
    }

    public Double getDurationMinute() {
        return durationMinute;
    }

    public void setDurationMinute(Double durationMinute) {
        this.durationMinute = durationMinute;
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

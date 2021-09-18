package com.github.zhangchunsheng.flink.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EquipmentWorkTimeEvent {
    // timestamp statusDuration lastTimestamp
    @SerializedName("package_date")
    private Integer packageDate;

    @SerializedName("start_package_time")
    private Long startPackageTime;

    @SerializedName("end_package_time")
    private Long endPackageTime;

    @SerializedName("status")
    private Integer status;

    @SerializedName("equipment_number")
    private String equipmentNumber;

    @SerializedName("status_duration")
    private Integer statusDuration;

    @SerializedName("duration_minute")
    private Double durationMinute;

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

    @SerializedName("count")
    private Integer count;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

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

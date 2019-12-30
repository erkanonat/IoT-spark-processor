package com.iot.spark.entity;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class AnomallyTrafficData {


    private String plateNumber;

    private String duplicates;

    private long totalCount;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    private Date timeStamp;

    private String recordDate;

    public String getDuplicates() {
        return duplicates;
    }

    public void setDuplicates(String duplicates) {
        this.duplicates = duplicates;
    }

    public String getPlateNumber() {
        return plateNumber;
    }

    public void setPlateNumber(String plateNumber) {
        this.plateNumber = plateNumber;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getRecordDate() {
        return recordDate;
    }

    public void setRecordDate(String recordDate) {
        this.recordDate = recordDate;
    }
}

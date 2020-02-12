package com.hvl.iot.bigdatastreaming.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.io.Serializable;
import java.util.Date;

public class WindowTrafficData implements Serializable {

    private String ptsId;
    private String vehicleType;
    private long totalCount;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    private Date timeStamp;
    private String recordDate;

    public String getPtsId() {
        return ptsId;
    }

    public void setPtsId(String ptsId) {
        this.ptsId = ptsId;
    }

    public String getVehicleType() {
        return vehicleType;
    }
    public void setVehicleType(String vehicleType) {
        this.vehicleType = vehicleType;
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

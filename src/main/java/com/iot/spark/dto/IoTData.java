package com.iot.spark.dto;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.io.Serializable;
import java.util.Date;

public class IoTData implements Serializable{

	private String ptsId;
	private String plateNumber;
	private String color;
	private double speed;
	private String vehicleType;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
	private Date timestamp;


	public IoTData(){

	}

	public IoTData(String ptsId, String plateNumber, String color, double speed, String vehicleType, Date timestamp) {

		super();
		this.ptsId = ptsId;
		this.plateNumber = plateNumber;
		this.color = color;
		this.speed = speed;
		this.vehicleType = vehicleType;
		this.timestamp = timestamp;
	}

	public String getPtsId() {
		return ptsId;
	}

	public void setPtsId(String ptsId) {
		this.ptsId = ptsId;
	}

	public String getPlateNumber() {
		return plateNumber;
	}

	public void setPlateNumber(String plateNumber) {
		this.plateNumber = plateNumber;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

	public String getVehicleType() {
		return vehicleType;
	}

	public void setVehicleType(String vehicleType) {
		this.vehicleType = vehicleType;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

}

package com.iot.spark.dto;

import java.io.Serializable;

public class AggregateKey implements Serializable {
	
	private String ptsId;
	private String vehicleType;
	
	public AggregateKey(String ptsId, String vehicleType) {
		super();
		this.ptsId = ptsId;
		this.vehicleType = vehicleType;
	}

	public String getPtsId() {
		return ptsId;
	}

	public String getVehicleType() {
		return vehicleType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ptsId == null) ? 0 : ptsId.hashCode());
		result = prime * result + ((vehicleType == null) ? 0 : vehicleType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj !=null && obj instanceof AggregateKey){
			AggregateKey other = (AggregateKey)obj;
			if(other.getPtsId() != null && other.getVehicleType() != null){
				if((other.getPtsId().equals(this.ptsId)) && (other.getVehicleType().equals(this.vehicleType))){
					return true;
				} 
			}
		}
		return false;
	}
	

}

package com.iot.spark.dto;

public class AggregateKey2 {

    private String duplicates;
    private String plateNumber;

    public AggregateKey2(String duplicates, String plateNumber) {
        super();
        this.duplicates = duplicates;
        this.plateNumber = plateNumber;
    }

    public String getDuplicates() {
        return duplicates;
    }

    public String getPlateNumber() {
        return plateNumber;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((duplicates == null) ? 0 : duplicates.hashCode());
        result = prime * result + ((plateNumber == null) ? 0 : plateNumber.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj !=null && obj instanceof AggregateKey2){
            AggregateKey2 other = (AggregateKey2)obj;
            if(other.getDuplicates() != null && other.getDuplicates() != null){
                if((other.getPlateNumber().equals(this.plateNumber)) && (other.getPlateNumber().equals(this.plateNumber))){
                    return true;
                }
            }
        }
        return false;
    }

}

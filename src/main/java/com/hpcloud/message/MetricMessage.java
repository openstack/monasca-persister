package com.hpcloud.message;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Map;

public class MetricMessage {

    @JsonProperty
    String name = null;

    @JsonProperty
    String tenant = null;

    @JsonProperty
    String region = null;

    @JsonProperty
    Map<String, String> dimensions = null;

    @JsonProperty
    String timeStamp = null;

    @JsonProperty
    Double value = null;

    @JsonProperty
    Double[][] timeValues = null;

    @Override
    public String toString() {
        return "MetricMessage{" +
                "name='" + name + '\'' +
                ", tenant='" + tenant + '\'' +
                ", region='" + region + '\'' +
                ", dimensions=" + dimensions +
                ", timeStamp='" + timeStamp + '\'' +
                ", value=" + value +
                ", timeValues=" + Arrays.toString(timeValues) +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = dimensions;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Double[][] getTimeValues() {
        return timeValues;
    }

    public void setTimeValues(Double[][] timeValues) {
        this.timeValues = timeValues;
    }
}

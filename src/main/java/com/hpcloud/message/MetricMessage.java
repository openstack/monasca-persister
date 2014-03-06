package com.hpcloud.message;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Map;

public class MetricMessage {

    @JsonProperty
    String name = null;

    @JsonProperty
    String region = "";

    @JsonProperty
    Map<String, String> dimensions = null;

    @JsonProperty
    String timestamp = null;

    @JsonProperty
    Double value = null;

    @JsonProperty
    Double[][] time_values = null;

    @Override
    public String toString() {
        return "MetricMessage{" +
                "name='" + name + '\'' +
                ", region='" + region + '\'' +
                ", dimensions=" + dimensions +
                ", timeStamp='" + timestamp + '\'' +
                ", value=" + value +
                ", time_values=" + Arrays.toString(time_values) +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        return timestamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timestamp = timeStamp;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Double[][] getTime_values() {
        return time_values;
    }

    public void setTime_values(Double[][] time_values) {
        this.time_values = time_values;
    }
}

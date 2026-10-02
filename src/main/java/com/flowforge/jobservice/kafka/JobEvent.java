package com.flowforge.jobservice.kafka;

public class JobEvent {

    private String jobId;
    private String type;
    private String payload;

    public JobEvent() {
    }

    public JobEvent(String jobId, String type, String payload) {
        this.jobId = jobId;
        this.type = type;
        this.payload = payload;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
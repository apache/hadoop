package org.apache.hadoop.fs.azurebfs.utils;

import java.util.UUID;

public class TrackingContext {
    private final String clientCorrelationID;
    private final String clientRequestID;

    public TrackingContext(String clientCorrelationID){
        clientRequestID = UUID.randomUUID().toString();
        this.clientCorrelationID = clientCorrelationID;
    }

    public String toString(){
        return clientCorrelationID + ":" + clientRequestID;
    }
}

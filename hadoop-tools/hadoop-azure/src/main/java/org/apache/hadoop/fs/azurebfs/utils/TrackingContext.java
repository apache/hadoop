package org.apache.hadoop.fs.azurebfs.utils;

import java.util.UUID;

public class TrackingContext {
    private final String clientCorrelationID;
    //private final String fileSystemID;
    private final String clientRequestID;
    //private final String primaryRequestID;
    //private final int retryNum;

    public TrackingContext(String clientCorrelationID){
        clientRequestID = UUID.randomUUID().toString();
        this.clientCorrelationID = clientCorrelationID;
    }

    public String toString(){
        return clientCorrelationID + ":" + clientRequestID;
    }
}

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;

public interface NodeHeartbeatResponse {
  public abstract HeartbeatResponse getHeartbeatResponse();
  
  public abstract void setHeartbeatResponse(HeartbeatResponse heartbeatResponse);
}

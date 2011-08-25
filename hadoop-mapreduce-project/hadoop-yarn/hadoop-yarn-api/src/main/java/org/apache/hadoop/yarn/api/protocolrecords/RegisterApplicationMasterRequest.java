package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

public interface RegisterApplicationMasterRequest {

  ApplicationAttemptId getApplicationAttemptId();
  void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  String getHost();
  void setHost(String host);

  int getRpcPort();
  void setRpcPort(int port);

  String getTrackingUrl();
  void setTrackingUrl(String string);
}

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;


public interface FinishApplicationMasterRequest {

  ApplicationAttemptId getApplicationAttemptId();
  void setAppAttemptId(ApplicationAttemptId applicationAttemptId);

  String getFinalState();
  void setFinalState(String string);

  String getDiagnostics();
  void setDiagnostics(String string);

  String getTrackingUrl();
  void setTrackingUrl(String historyUrl);

}

package org.apache.hadoop.yarn.api.protocolrecords;

public interface GetQueueInfoRequest {
  String getQueueName();
  void setQueueName(String queueName);

  boolean getIncludeApplications();
  void setIncludeApplications(boolean includeApplications);

  boolean getIncludeChildQueues();
  void setIncludeChildQueues(boolean includeChildQueues);

  boolean getRecursive();
  void setRecursive(boolean recursive);
}


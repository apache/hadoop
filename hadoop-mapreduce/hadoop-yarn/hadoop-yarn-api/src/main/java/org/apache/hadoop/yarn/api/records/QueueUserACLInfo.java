package org.apache.hadoop.yarn.api.records;

import java.util.List;

public interface QueueUserACLInfo {
  String getQueueName();
  void setQueueName(String queueName);
  
  List<QueueACL> getUserAcls();
  void setUserAcls(List<QueueACL> acls);
}

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.QueueInfo;

public interface GetQueueInfoResponse {
  QueueInfo getQueueInfo();
  void setQueueInfo(QueueInfo queueInfo);
}

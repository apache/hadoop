package org.apache.hadoop.yarn.api.records;

public interface NodeId extends Comparable<NodeId> {

  String getHost();
  void setHost(String host);

  int getPort();
  void setPort(int port);
}

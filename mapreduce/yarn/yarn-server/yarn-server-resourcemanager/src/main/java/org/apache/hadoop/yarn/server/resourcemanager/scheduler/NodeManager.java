package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;

public interface NodeManager extends NodeInfo {

  public static final String ANY = "*";

  boolean releaseContainer(Container container);

  void updateHealthStatus(NodeHealthStatus healthStatus);

  NodeResponse statusUpdate(Map<String, List<Container>> containers);

  void finishedApplication(ApplicationId applicationId);
}

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;


/**
 * The class that encapsulates response from clusterinfo for 
 * updates from the node managers.
 */
public class NodeResponse {
  private final List<Container> completed;
  private final List<Container> toCleanUp;
  private final List<ApplicationId> finishedApplications;
  
  public NodeResponse(List<ApplicationId> finishedApplications,
      List<Container> completed, List<Container> toKill) {
    this.finishedApplications = finishedApplications;
    this.completed = completed;
    this.toCleanUp = toKill;
  }
  public List<ApplicationId> getFinishedApplications() {
    return this.finishedApplications;
  }
  public List<Container> getCompletedContainers() {
    return this.completed;
  }
  public List<Container> getContainersToCleanUp() {
    return this.toCleanUp;
  }
}
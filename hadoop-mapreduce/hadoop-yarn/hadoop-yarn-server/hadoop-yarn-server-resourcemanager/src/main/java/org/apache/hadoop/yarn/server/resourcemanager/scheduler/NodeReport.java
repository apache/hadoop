package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * Node usage report.
 */
@Private
@Stable
public class NodeReport {
  private final Resource usedResources;
  private final int numContainers;
  
  public NodeReport(Resource used, int numContainers) {
    this.usedResources = used;
    this.numContainers = numContainers;
  }

  public Resource getUsedResources() {
    return usedResources;
  }

  public int getNumContainers() {
    return numContainers;
  }
}

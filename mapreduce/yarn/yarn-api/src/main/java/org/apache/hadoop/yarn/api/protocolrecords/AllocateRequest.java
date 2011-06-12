package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public interface AllocateRequest {
  public abstract ApplicationStatus getApplicationStatus();
  
  public abstract List<ResourceRequest> getAskList();
  public abstract ResourceRequest getAsk(int index);
  public abstract int getAskCount();
  
  public abstract List<Container> getReleaseList();
  public abstract Container getRelease(int index);
  public abstract int getReleaseCount();
  
  public abstract void setApplicationStatus(ApplicationStatus applicationStatus);
  
  public abstract void addAllAsks(List<ResourceRequest> resourceRequest);
  public abstract void addAsk(ResourceRequest request);
  public abstract void removeAsk(int index);
  public abstract void clearAsks();
  
  public abstract void addAllReleases(List<Container> releaseContainers);
  public abstract void addRelease(Container container);
  public abstract void removeRelease(int index);
  public abstract void clearReleases();
}

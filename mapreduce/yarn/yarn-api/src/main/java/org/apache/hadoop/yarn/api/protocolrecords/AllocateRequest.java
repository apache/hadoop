package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public interface AllocateRequest {

  ApplicationAttemptId getApplicationAttemptId();
  void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  int getResponseId();
  void setResponseId(int id);

  float getProgress();
  void setProgress(float progress);

  List<ResourceRequest> getAskList();
  ResourceRequest getAsk(int index);
  int getAskCount();
  
  List<Container> getReleaseList();
  Container getRelease(int index);
  int getReleaseCount();

  void addAllAsks(List<ResourceRequest> resourceRequest);
  void addAsk(ResourceRequest request);
  void removeAsk(int index);
  void clearAsks();
  
  void addAllReleases(List<Container> releaseContainers);
  void addRelease(Container container);
  void removeRelease(int index);
  void clearReleases();
}

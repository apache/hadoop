package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;

public interface RMAppAttempt extends EventHandler<RMAppAttemptEvent>{

  ApplicationAttemptId getAppAttemptId();

  RMAppAttemptState getAppAttemptState();

  String getHost();

  int getRpcPort();

  String getTrackingUrl();

  String getClientToken();

  StringBuilder getDiagnostics();

  float getProgress();

  Set<NodeId> getRanNodes();

  List<Container> pullJustFinishedContainers();

  List<Container> pullNewlyAllocatedContainers();

  Container getMasterContainer();

  ApplicationSubmissionContext getSubmissionContext();
}

package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerTALaunchRequestEvent;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerLaunchRequestEvent extends AMContainerEvent {

  // Temporarily sending in the event from the task.
//  private final ContainerLaunchContext clc;
  private final AMSchedulerTALaunchRequestEvent event;
  private final Map<ApplicationAccessType, String> applicationAcls;
  private final JobId jobId;
  
  public AMContainerLaunchRequestEvent(ContainerId containerId, AMSchedulerTALaunchRequestEvent event, Map<ApplicationAccessType, String> applicationAcls, JobId jobId) {
    super(containerId, AMContainerEventType.C_START_REQUEST);
    this.event = event;
    this.applicationAcls = applicationAcls;
    this.jobId = jobId;
  }
  
  // TODO XXX: Temporary. 
  public AMSchedulerTALaunchRequestEvent getLaunchRequestEvent() {
    return event;
  }
  
  public Map<ApplicationAccessType, String> getApplicationAcls() {
    return this.applicationAcls;
  }

  public JobId getJobId() {
    return this.jobId;
  }
}

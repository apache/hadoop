package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttempt;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.Resource;

public class AMSchedulerTALaunchRequestEvent extends AMSchedulerEvent {

  // TODO XXX: Get rid of remoteTask from here. Can be forgottent after it has been assigned.
  //.... Maybe have the Container talk to the TaskAttempt to pull in the remote task.
  
  private final TaskAttemptId attemptId;
  private final boolean rescheduled;
  private final Resource capability;
  private final org.apache.hadoop.mapred.Task remoteTask;
  private final TaskAttempt taskAttempt;
  private final Credentials credentials;
  private Token<JobTokenIdentifier> jobToken;
  private final String[] hosts;
  private final String[] racks;
  
  
  public AMSchedulerTALaunchRequestEvent(TaskAttemptId attemptId,
      boolean rescheduled, Resource capability,
      org.apache.hadoop.mapred.Task remoteTask, TaskAttempt ta,
      Credentials credentials, Token<JobTokenIdentifier> jobToken,
      String[] hosts, String[] racks) {
    super(AMSchedulerEventType.S_TA_LAUNCH_REQUEST);
    this.attemptId = attemptId;
    this.rescheduled = rescheduled;
    this.capability = capability;
    this.remoteTask = remoteTask;
    this.taskAttempt = ta;
    this.credentials = credentials;
    this.jobToken = jobToken;
    this.hosts = hosts;
    this.racks = racks;
  }

  public TaskAttemptId getAttemptID() {
    return this.attemptId;
  }

  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }
  
  public String[] getRacks() {
    return racks;
  }
  
  public boolean isRescheduled() {
    return rescheduled;
  }
  
  public org.apache.hadoop.mapred.Task getRemoteTask() {
    return remoteTask;
  }
  
  public TaskAttempt getTaskAttempt() {
    return this.taskAttempt;
  }
  
  public Credentials getCredentials() {
    return this.credentials;
  }
  
  public Token<JobTokenIdentifier> getJobToken() {
    return this.jobToken;
  }
 
  //TODO Passing taskAttempt for now. Required params from the call to createContainerLaunchContext
  /*
  ContainerLaunchContext clc = TaskAttemptImplHelpers.createContainerLaunchContext(null, null, ta.conf, ta.jobToken, ta.remoteTask, ta.oldJobId, null, null, ta.taskAttemptListener, ta.credentials)
      //TODO WTF are the AppACLs coming from
      // containerId not present.
      // assignedCapability versus requested capability.
      // jvmId isn't known at this point. Dependent on the containerId.
      // TaskAttemptListener may not be required at this point.
  */

  //XXX jvmId is required in the launch context - to construct the command line.
  //XXX Parameter replacement: @taskid@ will not be usable
  //XXX ProfileTaskRange not available along with ContainerReUse
  //XXX TaskAttemptId - firstTaskId used in YarnChild to setup JvmId, metrics etc.
  // For now, will differentiating based on Map/Reduce task.
  
  /*Requirements to determine a container request.
   * + Data-local + Rack-local hosts.
   * + Resource capability
   * + Env - mapreduce.map.env / mapreduce.reduce.env can change. M/R log level. 
   * - JobConf and JobJar file - same location.
   * - Distributed Cache - identical for map / reduce tasks at the moment.
   * - Credentials, tokens etc are identical.
   * + Command - dependent on map / reduce java.opts
   */
}
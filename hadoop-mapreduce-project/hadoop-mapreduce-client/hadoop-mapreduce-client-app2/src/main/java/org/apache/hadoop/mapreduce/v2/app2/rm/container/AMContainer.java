package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;

public interface AMContainer extends EventHandler<AMContainerEvent>{
  
  public AMContainerState getState();
  public ContainerId getContainerId();
  public Container getContainer();
  //TODO Rename - CompletedTaskAttempts, ideally means FAILED / KILLED as well.
  public List<TaskAttemptId> getCompletedTaskAttempts();
  public TaskAttemptId getRunningTaskAttempt();
  public List<TaskAttemptId> getQueuedTaskAttempts();
  
  public int getShufflePort();
  
  // TODO Add a method to get the containers capabilities - to match taskAttempts.

}

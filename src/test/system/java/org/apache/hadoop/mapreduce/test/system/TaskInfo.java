package org.apache.hadoop.mapreduce.test.system;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskID;

/**
 * Task state information of a TaskInProgress as seen by the {@link JobTracker}
 */
public interface TaskInfo extends Writable {
  /**
   * Gets the task id of the TaskInProgress.
   * 
   * @return id of the task.
   */
  TaskID getTaskID();

  /**
   * Number of times task attempts have failed for the given TaskInProgress.
   * <br/>
   * 
   * @return number of failed task attempts.
   */
  int numFailedAttempts();

  /**
   * Number of times task attempts have been killed for the given TaskInProgress 
   * <br/>
   * 
   * @return number of killed task attempts.
   */
  int numKilledAttempts();

  /**
   * Gets the progress of the Task in percentage will be in range of 0.0-1.0 
   * <br/>
   * 
   * @return progress of task in percentage.
   */
  double getProgress();

  /**
   * Number of attempts currently running for the given TaskInProgress.<br/>
   * 
   * @return number of running attempts.
   */
  int numRunningAttempts();

  /**
   * Array of TaskStatus objects that are related to the corresponding
   * TaskInProgress object.
   * 
   * @return
   */
  TaskStatus[] getTaskStatus();
}

package org.apache.hadoop.mapreduce.test.system;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskTracker;

/**
 * Task state information as seen by the TT.
 */
public interface TTTaskInfo extends Writable {

  /**
   * Gets the diagnostic information associated the the task.<br/>
   * 
   * @return diagnostic information of the task.
   */
  String getDiagnosticInfo();

  /**
   * Has task occupied a slot? A task occupies a slot once it starts localizing
   * on the {@link TaskTracker} <br/>
   * 
   * @return true if task has started occupying a slot.
   */
  boolean slotTaken();

  /**
   * Has the task been killed? <br/>
   * 
   * @return true, if task has been killed.
   */
  boolean wasKilled();

  /**
   * Gets the task status associated with the particular task trackers task 
   * view.<br/>
   * 
   * @return status of the particular task
   */
  TaskStatus getTaskStatus();
}
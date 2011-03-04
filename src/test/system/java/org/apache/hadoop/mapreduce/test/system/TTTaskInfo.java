package org.apache.hadoop.mapreduce.test.system;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskTracker;

/**
 * Task state information as seen by the TT.
 */
public interface TTTaskInfo extends Writable {
  /**
   * Gets the task associated to the instance as seen by {@link TaskTracker}
   * <br/>
   * 
   * @return task.
   */
  Task getTask();

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
}
package org.apache.hadoop.mapreduce.test.system;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;

/**
 * TaskTracker state information as seen by the JobTracker.
 */
public interface TTInfo extends Writable {
  /**
   * Gets the {@link TaskTracker} name.<br/>
   * 
   * @return name of the tracker.
   */
  String getName();

  /**
   * Gets the current status of the {@link TaskTracker} <br/>
   * 
   * @return status of the {@link TaskTracker}
   */
  TaskTrackerStatus getStatus();
}
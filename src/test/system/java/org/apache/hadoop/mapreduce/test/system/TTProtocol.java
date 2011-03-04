package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;

import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.test.system.DaemonProtocol;

/**
 * TaskTracker RPC interface to be used for cluster tests.
 */
public interface TTProtocol extends DaemonProtocol {

  public static final long versionID = 1L;
  /**
   * Gets latest status which was sent in heartbeat to the {@link JobTracker}. 
   * <br/>
   * 
   * @return status
   * @throws IOException
   */
  TaskTrackerStatus getStatus() throws IOException;

  /**
   * Gets list of all the tasks in the {@link TaskTracker}.<br/>
   * 
   * @return list of all the tasks
   * @throws IOException
   */
  TTTaskInfo[] getTasks() throws IOException;
}

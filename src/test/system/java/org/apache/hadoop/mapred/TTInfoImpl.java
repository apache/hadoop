package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapreduce.test.system.TTInfo;

/**
 * Concrete implementation of the TaskTracker information which is passed to 
 * the client from JobTracker.
 * Look at {@link TTInfo}
 */

class TTInfoImpl implements TTInfo {

  private String taskTrackerName;
  private TaskTrackerStatus status;

  public TTInfoImpl() {
    taskTrackerName = "";
    status = new TaskTrackerStatus();
  }
  
  public TTInfoImpl(String taskTrackerName, TaskTrackerStatus status) {
    super();
    this.taskTrackerName = taskTrackerName;
    this.status = status;
  }

  @Override
  public String getName() {
    return taskTrackerName;
  }

  @Override
  public TaskTrackerStatus getStatus() {
    return status;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    taskTrackerName = in.readUTF();
    status.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(taskTrackerName);
    status.write(out);
  }

}

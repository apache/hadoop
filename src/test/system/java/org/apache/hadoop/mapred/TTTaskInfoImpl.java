package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.test.system.TTTaskInfo;
/**
 * Abstract class which passes the Task view of the TaskTracker to the client.
 * See {@link TTInfoImpl} for further details.
 *
 */
abstract class TTTaskInfoImpl implements TTTaskInfo {

  private String diagonsticInfo;
  private boolean slotTaken;
  private boolean wasKilled;
  TaskStatus status;

  public TTTaskInfoImpl() {
  }

  public TTTaskInfoImpl(boolean slotTaken, boolean wasKilled,
      String diagonsticInfo, TaskStatus status) {
    super();
    this.diagonsticInfo = diagonsticInfo;
    this.slotTaken = slotTaken;
    this.wasKilled = wasKilled;
    this.status = status;
  }

  @Override
  public String getDiagnosticInfo() {
    return diagonsticInfo;
  }

  @Override
  public boolean slotTaken() {
    return slotTaken;
  }

  @Override
  public boolean wasKilled() {
    return wasKilled;
  }

  @Override
  public TaskStatus getTaskStatus() {
    return status;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    slotTaken = in.readBoolean();
    wasKilled = in.readBoolean();
    diagonsticInfo = in.readUTF();
    status.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(slotTaken);
    out.writeBoolean(wasKilled);
    out.writeUTF(diagonsticInfo);
    status.write(out);
  }

  static class MapTTTaskInfo extends TTTaskInfoImpl {

    public MapTTTaskInfo() {
      super(false, false, "", new MapTaskStatus());
    }

    public MapTTTaskInfo(boolean slotTaken, boolean wasKilled,
        String diagonsticInfo, MapTaskStatus status) {
      super(slotTaken, wasKilled, diagonsticInfo, status);
    }
  }

  static class ReduceTTTaskInfo extends TTTaskInfoImpl {

    public ReduceTTTaskInfo() {
      super(false, false, "", new ReduceTaskStatus());
    }

    public ReduceTTTaskInfo(boolean slotTaken,
        boolean wasKilled, String diagonsticInfo,ReduceTaskStatus status) {
      super(slotTaken, wasKilled, diagonsticInfo, status);
    }
  }

}

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
  private Task task;
  private boolean slotTaken;
  private boolean wasKilled;

  public TTTaskInfoImpl() {
  }

  public TTTaskInfoImpl(Task task, boolean slotTaken, boolean wasKilled,
      String diagonsticInfo) {
    super();
    this.diagonsticInfo = diagonsticInfo;
    this.task = task;
    this.slotTaken = slotTaken;
    this.wasKilled = wasKilled;
  }

  @Override
  public String getDiagnosticInfo() {
    return diagonsticInfo;
  }

  @Override
  public Task getTask() {
    return task;
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
  public void readFields(DataInput in) throws IOException {
    task.readFields(in);
    slotTaken = in.readBoolean();
    wasKilled = in.readBoolean();
    diagonsticInfo = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    task.write(out);
    out.writeBoolean(slotTaken);
    out.writeBoolean(wasKilled);
    out.writeUTF(diagonsticInfo);
  }

  static class MapTTTaskInfo extends TTTaskInfoImpl {

    public MapTTTaskInfo() {
      super(new MapTask(), false, false, "");
    }

    public MapTTTaskInfo(MapTask task, boolean slotTaken, boolean wasKilled,
        String diagonsticInfo) {
      super(task, slotTaken, wasKilled, diagonsticInfo);
    }
  }

  static class ReduceTTTaskInfo extends TTTaskInfoImpl {

    public ReduceTTTaskInfo() {
      super(new ReduceTask(), false, false, "");
    }

    public ReduceTTTaskInfo(ReduceTask task, boolean slotTaken,
        boolean wasKilled, String diagonsticInfo) {
      super(task, slotTaken, wasKilled, diagonsticInfo);
    }

  }

}

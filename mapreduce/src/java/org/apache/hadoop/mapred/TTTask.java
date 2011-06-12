package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public abstract class TTTask implements Writable {
  
  protected Task task;
  
  public TTTask(Task task) {
    this.task = task;
  }
  
  /**
   * Return the task to be run
   * @return task to be run
   */
  public Task getTask() {
    return task;
  }
  
  /** 
   * Return an approprate thread runner for this task. 
   * @param tracker
   * @param tip
   * @param rjob
   * @return
   * @throws IOException
   */
  public abstract TaskRunner createRunner(TaskTracker tracker, 
                                          TaskTracker.TaskInProgress tip, 
                                          TaskTracker.RunningJob rjob
                                          ) throws IOException;

  public void write(DataOutput out) throws IOException {
    out.writeBoolean(task.isMapTask());
    if (!task.isMapTask()) {
      // which flavor of ReduceTask, uber or regular?
      out.writeBoolean(task.isUberTask());
    }
    task.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    boolean isMapTask = in.readBoolean();
    if (isMapTask) {
      task = new MapTask();
    } else {
      boolean isUberTask = in.readBoolean();
      if (isUberTask) {
        task = new UberTask();
      } else {
        task = new ReduceTask();
      }
    }
    task.readFields(in);
  }

}

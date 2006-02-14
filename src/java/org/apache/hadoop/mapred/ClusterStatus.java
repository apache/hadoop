package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableFactories;

/**
 * Summarizes the size and current state of the cluster.
 * @author Owen O'Malley
 */
public class ClusterStatus implements Writable {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ClusterStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new ClusterStatus(); }
       });
    }

  private int task_trackers;
  private int map_tasks;
  private int reduce_tasks;
  private int max_tasks;

  private ClusterStatus() {}
  
  ClusterStatus(int trackers, int maps, int reduces, int max) {
    task_trackers = trackers;
    map_tasks = maps;
    reduce_tasks = reduces;
    max_tasks = max;
  }
  

  /**
   * The number of task trackers in the cluster.
   */
  public int getTaskTrackers() {
    return task_trackers;
  }
  
  /**
   * The number of currently running map tasks.
   */
  public int getMapTasks() {
    return map_tasks;
  }
  
  /**
   * The number of current running reduce tasks.
   */
  public int getReduceTasks() {
    return reduce_tasks;
  }
  
  /**
   * The maximum capacity for running tasks in the cluster.
   */
  public int getMaxTasks() {
    return max_tasks;
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(task_trackers);
    out.writeInt(map_tasks);
    out.writeInt(reduce_tasks);
    out.writeInt(max_tasks);
  }

  public void readFields(DataInput in) throws IOException {
    task_trackers = in.readInt();
    map_tasks = in.readInt();
    reduce_tasks = in.readInt();
    max_tasks = in.readInt();
  }

}

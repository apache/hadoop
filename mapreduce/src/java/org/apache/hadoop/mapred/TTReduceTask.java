package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;

public class TTReduceTask extends TTTask {

  public TTReduceTask(ReduceTask reduceTask) {
    super(reduceTask);
  }
  
  @Override
  public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip,
                                 TaskTracker.RunningJob rjob
                                 ) throws IOException {
    return new ReduceTaskRunner(tip, tracker, task.conf, rjob);
  }

}

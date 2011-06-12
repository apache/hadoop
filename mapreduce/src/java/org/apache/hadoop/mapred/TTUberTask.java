package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;

public class TTUberTask extends TTTask {

  public TTUberTask(UberTask task) {
    super(task);
  }

  @Override
  public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip,
      TaskTracker.RunningJob rjob) throws IOException {
    return new UberTaskRunner(tip, tracker, task.conf, rjob);
  }

}

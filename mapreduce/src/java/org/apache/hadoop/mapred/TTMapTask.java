package org.apache.hadoop.mapred;

import java.io.IOException;

public class TTMapTask extends TTTask {
  
  public TTMapTask(MapTask mapTask) {
    super(mapTask);
  }
  
  @Override
  public TaskRunner createRunner(TaskTracker tracker, 
                                 TaskTracker.TaskInProgress tip,
                                 TaskTracker.RunningJob rjob
                                 ) throws IOException {
    return new MapTaskRunner(tip, tracker, task.conf, rjob);
  }
  
}

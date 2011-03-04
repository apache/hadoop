package org.apache.hadoop.mapreduce.test.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.test.system.ControlAction;

/**
 * Control Action which signals a controlled task to proceed to completion. <br/>
 */
public class FinishTaskControlAction extends ControlAction<TaskID> {

  private static final String ENABLE_CONTROLLED_TASK_COMPLETION =
      "test.system.enabled.task.completion.control";

  /**
   * Create a default control action. <br/>
   * 
   */
  public FinishTaskControlAction() {
    super(new TaskID());
  }

  /**
   * Create a control action specific to a particular task. <br/>
   * 
   * @param id
   *          of the task.
   */
  public FinishTaskControlAction(TaskID id) {
    super(id);
  }

  /**
   * Sets up the job to be controlled using the finish task control action. 
   * <br/>
   * 
   * @param conf
   *          configuration to be used submit the job.
   */
  public static void configureControlActionForJob(Configuration conf) {
    conf.setBoolean(ENABLE_CONTROLLED_TASK_COMPLETION, true);
  }
  
  /**
   * Checks if the control action is enabled in the passed configuration. <br/>
   * @param conf configuration
   * @return true if action is enabled.
   */
  public static boolean isControlActionEnabled(Configuration conf) {
    return conf.getBoolean(ENABLE_CONTROLLED_TASK_COMPLETION, false);
  }
}

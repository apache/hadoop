/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;


import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The default implementation for controlling tasks.
 * 
 * This class provides an implementation for launching and killing 
 * tasks that need to be run as the tasktracker itself. Hence,
 * many of the initializing or cleanup methods are not required here.
 */
class DefaultTaskController extends TaskController {

  private static final Log LOG = 
      LogFactory.getLog(DefaultTaskController.class);
  /**
   * Launch a new JVM for the task.
   * 
   * This method launches the new JVM for the task by executing the
   * the JVM command using the {@link Shell.ShellCommandExecutor}
   */
  void launchTaskJVM(TaskController.TaskControllerContext context) 
                                      throws IOException {
    JvmEnv env = context.env;
    List<String> wrappedCommand = 
      TaskLog.captureOutAndError(env.setup, env.vargs, env.stdout, env.stderr,
          env.logSize, env.pidFile);
    ShellCommandExecutor shexec = 
        new ShellCommandExecutor(wrappedCommand.toArray(new String[0]), 
                                  env.workDir, env.env);
    // set the ShellCommandExecutor for later use.
    context.shExec = shexec;
    shexec.execute();
  }
  
  /**
   * Kills the JVM running the task stored in the context.
   * 
   * @param context the context storing the task running within the JVM
   * that needs to be killed.
   */
  void killTaskJVM(TaskController.TaskControllerContext context) {
    ShellCommandExecutor shexec = context.shExec;
    if (shexec != null) {
      Process process = shexec.getProcess();
      if (process != null) {
        process.destroy();
      }
    }
  }
  
  /**
   * Initialize the task environment.
   * 
   * Since tasks are launched as the tasktracker user itself, this
   * method has no action to perform.
   */
  void initializeTask(TaskController.TaskControllerContext context) {
    // The default task controller does not need to set up
    // any permissions for proper execution.
    // So this is a dummy method.
    return;
  }
  

  @Override
  void setup() {
    // nothing to setup
    return;
  }

  /*
   * No need to do anything as we don't need to do as we dont need anything
   * extra from what TaskTracker has done.
   */
  @Override
  void initializeJob(JobID jobId) {
  }
  
}

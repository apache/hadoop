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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * Controls initialization, finalization and clean up of tasks, and
 * also the launching and killing of task JVMs.
 * 
 * This class defines the API for initializing, finalizing and cleaning
 * up of tasks, as also the launching and killing task JVMs.
 * Subclasses of this class will implement the logic required for
 * performing the actual actions. 
 */
abstract class TaskController implements Configurable {
  
  private Configuration conf;
  
  public static final Log LOG = LogFactory.getLog(TaskController.class);
  
  public Configuration getConf() {
    return conf;
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Setup task controller component.
   * 
   */
  abstract void setup();
  
  
  /**
   * Launch a task JVM
   * 
   * This method defines how a JVM will be launched to run a task.
   * @param context the context associated to the task
   */
  abstract void launchTaskJVM(TaskControllerContext context)
                                      throws IOException;
  
  /**
   * Top level cleanup a task JVM method.
   *
   * The current implementation does the following.
   * <ol>
   * <li>Sends a graceful terminate signal to task JVM allowing its sub-process
   * to cleanup.</li>
   * <li>Waits for stipulated period</li>
   * <li>Sends a forceful kill signal to task JVM, terminating all its
   * sub-process forcefully.</li>
   * </ol>
   * 
   * @param context the task for which kill signal has to be sent.
   */
  final void destroyTaskJVM(TaskControllerContext context) {
    terminateTask(context);
    try {
      Thread.sleep(context.sleeptimeBeforeSigkill);
    } catch (InterruptedException e) {
      LOG.warn("Sleep interrupted : " + 
          StringUtils.stringifyException(e));
    }
    killTask(context);
  }
  
  /**
   * Perform initializing actions required before a task can run.
   * 
   * For instance, this method can be used to setup appropriate
   * access permissions for files and directories that will be
   * used by tasks. Tasks use the job cache, log, PID and distributed cache
   * directories and files as part of their functioning. Typically,
   * these files are shared between the daemon and the tasks
   * themselves. So, a TaskController that is launching tasks
   * as different users can implement this method to setup
   * appropriate ownership and permissions for these directories
   * and files.
   */
  abstract void initializeTask(TaskControllerContext context);
  
  
  /**
   * Contains task information required for the task controller.  
   */
  static class TaskControllerContext {
    // task being executed
    Task task; 
    // the JVM environment for the task
    JvmEnv env;
    // the Shell executor executing the JVM for this task
    ShellCommandExecutor shExec; 
    // process handle of task JVM
    String pid;
    // waiting time before sending SIGKILL to task JVM after sending SIGTERM
    long sleeptimeBeforeSigkill;
  }

  /**
   * Contains info related to the path of the file/dir to be deleted. This info
   * is needed by task-controller to build the full path of the file/dir
   */
  static class TaskControllerPathDeletionContext extends PathDeletionContext {
    Task task;
    boolean isWorkDir;
    TaskController taskController;

    /**
     * mapredLocalDir is the base dir under which to-be-deleted taskWorkDir or
     * taskAttemptDir exists. fullPath of taskAttemptDir or taskWorkDir
     * is built using mapredLocalDir, jobId, taskId, etc.
     */
    Path mapredLocalDir;

    public TaskControllerPathDeletionContext(FileSystem fs, Path mapredLocalDir,
        Task task, boolean isWorkDir, TaskController taskController) {
      super(fs, null);
      this.task = task;
      this.isWorkDir = isWorkDir;
      this.taskController = taskController;
      this.mapredLocalDir = mapredLocalDir;
    }

    @Override
    protected String getPathForCleanup() {
      if (fullPath == null) {
        fullPath = buildPathForDeletion();
      }
      return fullPath;
    }

    /**
     * Builds the path of taskAttemptDir OR taskWorkDir based on
     * mapredLocalDir, jobId, taskId, etc
     */
    String buildPathForDeletion() {
      String subDir = TaskTracker.getLocalTaskDir(task.getJobID().toString(),
          task.getTaskID().toString(), task.isTaskCleanupTask());
      if (isWorkDir) {
        subDir = subDir + Path.SEPARATOR + "work";
      }
      return mapredLocalDir.toUri().getPath() + Path.SEPARATOR + subDir;
    }

    /**
     * Makes the path(and its subdirectories recursively) fully deletable by
     * setting proper permissions(777) by task-controller
     */
    @Override
    protected void enablePathForCleanup() throws IOException {
      getPathForCleanup();// allow init of fullPath
      if (fs.exists(new Path(fullPath))) {
        taskController.enableTaskForCleanup(this); 
      }
    }
  }

  /**
   * Method which is called after the job is localized so that task controllers
   * can implement their own job localization logic.
   * 
   * @param tip  Task of job for which localization happens.
   */
  abstract void initializeJob(JobID jobId);
  
  /**
   * Sends a graceful terminate signal to taskJVM and it sub-processes. 
   *   
   * @param context task context
   */
  abstract void terminateTask(TaskControllerContext context);
  
  /**
   * Sends a KILL signal to forcefully terminate the taskJVM and its
   * sub-processes.
   * 
   * @param context task context
   */
  
  abstract void killTask(TaskControllerContext context);
  
  /**
   * Enable the task for cleanup by changing permissions of the path
   * @param context   path deletion context
   * @throws IOException
   */
  abstract void enableTaskForCleanup(PathDeletionContext context)
      throws IOException;
}

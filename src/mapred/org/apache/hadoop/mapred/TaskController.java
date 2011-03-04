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

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
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
 * 
 * <br/>
 * 
 * NOTE: This class is internal only class and not intended for users!!
 */
public abstract class TaskController implements Configurable {
  
  private Configuration conf;
  
  public static final Log LOG = LogFactory.getLog(TaskController.class);
  
  public Configuration getConf() {
    return conf;
  }

  // The list of directory paths specified in the variable mapred.local.dir.
  // This is used to determine which among the list of directories is picked up
  // for storing data for a particular task.
  protected String[] mapredLocalDirs;

  public void setConf(Configuration conf) {
    this.conf = conf;
    mapredLocalDirs = conf.getStrings("mapred.local.dir");
  }

  /**
   * Sets up the permissions of the following directories on all the configured
   * disks:
   * <ul>
   * <li>mapred-local directories</li>
   * <li>Hadoop log directories</li>
   * </ul>
   */
  public void setup() throws IOException {
    for (String localDir : this.mapredLocalDirs) {
      // Set up the mapred-local directories.
      File mapredlocalDir = new File(localDir);
      if (!mapredlocalDir.exists() && !mapredlocalDir.mkdirs()) {
        LOG.warn("Unable to create mapred-local directory : "
            + mapredlocalDir.getPath());
      } else {
        Localizer.PermissionsHandler.setPermissions(mapredlocalDir,
            Localizer.PermissionsHandler.sevenFiveFive);
      }
    }

    // Set up the user log directory
    File taskLog = TaskLog.getUserLogDir();
    if (!taskLog.exists() && !taskLog.mkdirs()) {
      LOG.warn("Unable to create taskLog directory : " + taskLog.getPath());
    } else {
      Localizer.PermissionsHandler.setPermissions(taskLog,
          Localizer.PermissionsHandler.sevenFiveFive);
    }
  }

  /**
   * Take task-controller specific actions to initialize job. This involves
   * setting appropriate permissions to job-files so as to secure the files to
   * be accessible only by the user's tasks.
   * 
   * @throws IOException
   */
  abstract void initializeJob(JobInitializationContext context) throws IOException;

  /**
   * Take task-controller specific actions to initialize the distributed cache
   * file. This involves setting appropriate permissions for these files so as
   * to secure them to be accessible only their owners.
   * 
   * @param context
   * @throws IOException
   */
  public abstract void initializeDistributedCacheFile(DistributedCacheFileContext context)
      throws IOException;

  /**
   * Launch a task JVM
   * 
   * This method defines how a JVM will be launched to run a task. Each
   * task-controller should also do an
   * {@link #initializeTask(TaskControllerContext)} inside this method so as to
   * initialize the task before launching it. This is for reasons of
   * task-controller specific optimizations w.r.t combining initialization and
   * launching of tasks.
   * 
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

  /** Perform initializing actions required before a task can run.
    * 
    * For instance, this method can be used to setup appropriate
    * access permissions for files and directories that will be
    * used by tasks. Tasks use the job cache, log, and distributed cache
    * directories and files as part of their functioning. Typically,
    * these files are shared between the daemon and the tasks
    * themselves. So, a TaskController that is launching tasks
    * as different users can implement this method to setup
    * appropriate ownership and permissions for these directories
    * and files.
    */
  abstract void initializeTask(TaskControllerContext context)
      throws IOException;

  /**
   * Contains task information required for the task controller.  
   */
  static class TaskControllerContext {
    // task being executed
    Task task;
    ShellCommandExecutor shExec;     // the Shell executor executing the JVM for this task.

    // Information used only when this context is used for launching new tasks.
    JvmEnv env;     // the JVM environment for the task.

    // Information used only when this context is used for destroying a task jvm.
    String pid; // process handle of task JVM.
    long sleeptimeBeforeSigkill; // waiting time before sending SIGKILL to task JVM after sending SIGTERM
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
      String subDir = (isWorkDir) ? TaskTracker.getTaskWorkDir(task.getUser(),
          task.getJobID().toString(), task.getTaskID().toString(),
          task.isTaskCleanupTask())
        : TaskTracker.getLocalTaskDir(task.getUser(),
          task.getJobID().toString(), task.getTaskID().toString(),
          task.isTaskCleanupTask());

      return mapredLocalDir.toUri().getPath() + Path.SEPARATOR + subDir;
    }

    /**
     * Makes the path(and its subdirectories recursively) fully deletable by
     * setting proper permissions(770) by task-controller
     */
    @Override
    protected void enablePathForCleanup() throws IOException {
      getPathForCleanup();// allow init of fullPath, if not inited already
      if (fs.exists(new Path(fullPath))) {
        taskController.enableTaskForCleanup(this);
      }
    }
  }

  /**
   * NOTE: This class is internal only class and not intended for users!!
   * 
   */
  public static class InitializationContext {
    public File workDir;
    public String user;
    
    public InitializationContext() {
    }
    
    public InitializationContext(String user, File workDir) {
      this.user = user;
      this.workDir = workDir;
    }
  }
  
  /**
   * This is used for initializing the private localized files in distributed
   * cache. Initialization would involve changing permission, ownership and etc.
   */
  public static class DistributedCacheFileContext extends InitializationContext {
    // base directory under which file has been localized
    Path localizedBaseDir;
    // the unique string used to construct the localized path
    String uniqueString;

    public DistributedCacheFileContext(String user, File workDir,
        Path localizedBaseDir, String uniqueString) {
      super(user, workDir);
      this.localizedBaseDir = localizedBaseDir;
      this.uniqueString = uniqueString;
    }

    public Path getLocalizedUniqueDir() {
      return new Path(localizedBaseDir, new Path(TaskTracker
          .getPrivateDistributedCacheDir(user), uniqueString));
    }
  }

  static class JobInitializationContext extends InitializationContext {
    JobID jobid;
  }

  /**
   * Sends a graceful terminate signal to taskJVM and it sub-processes. 
   *   
   * @param context task context
   */
  abstract void terminateTask(TaskControllerContext context);
  
  /**
   * Enable the task for cleanup by changing permissions of the path
   * @param context   path deletion context
   * @throws IOException
   */
  abstract void enableTaskForCleanup(PathDeletionContext context)
      throws IOException;
  /**
   * Sends a KILL signal to forcefully terminate the taskJVM and its
   * sub-processes.
   * 
   * @param context task context
   */
  abstract void killTask(TaskControllerContext context);

  /**
   * Initialize user on this TaskTracer in a TaskController specific manner.
   * 
   * @param context
   * @throws IOException
   */
  public abstract void initializeUser(InitializationContext context)
      throws IOException;
}

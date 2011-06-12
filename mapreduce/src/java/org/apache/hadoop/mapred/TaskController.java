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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.CleanupQueue.PathDeletionContext;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

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
 */
@InterfaceAudience.Private
public abstract class TaskController implements Configurable {
  
  private Configuration conf;
  
  public static final Log LOG = LogFactory.getLog(TaskController.class);
  
  public Configuration getConf() {
    return conf;
  }

  // The list of directory paths specified in the variable Configs.LOCAL_DIR
  // This is used to determine which among the list of directories is picked up
  // for storing data for a particular task.
  protected String[] mapredLocalDirs;

  public void setConf(Configuration conf) {
    this.conf = conf;
    mapredLocalDirs = conf.getTrimmedStrings(MRConfig.LOCAL_DIR);
  }

  /**
   * Sets up the permissions of the following directories on all the configured
   * disks:
   * <ul>
   * <li>mapreduce.cluster.local.directories</li>
   * <li>Hadoop log directories</li>
   * </ul>
   */
  public void setup() throws IOException {
    for (String localDir : this.mapredLocalDirs) {
      // Set up the mapreduce.cluster.local.directories.
      File mapredlocalDir = new File(localDir);
      if (!mapredlocalDir.exists() && !mapredlocalDir.mkdirs()) {
        LOG.warn("Unable to create mapreduce.cluster.local.directory : "
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
   * <ol>
   * <li>Sends a graceful termiante signal to task JVM to allow subprocesses
   * to cleanup.</li>
   * <li>Sends a forceful kill signal to task JVM, terminating all its
   * sub-processes forcefully.</li>
   * </ol>
   *
   * @param context the task for which kill signal has to be sent.
   */
  final void destroyTaskJVM(TaskControllerContext context) {
    // Send SIGTERM to try to ask for a polite exit.
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

  static class TaskExecContext {
    // task being executed
    Task task;
  }
  /**
   * Contains task information required for the task controller.  
   */
  static class TaskControllerContext extends TaskExecContext {
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
  static abstract class TaskControllerPathDeletionContext 
  extends PathDeletionContext {
    TaskController taskController;
    String user;

    /**
     * mapredLocalDir is the base dir under which to-be-deleted jobLocalDir, 
     * taskWorkDir or taskAttemptDir exists. fullPath of jobLocalDir, 
     * taskAttemptDir or taskWorkDir is built using mapredLocalDir, jobId, 
     * taskId, etc.
     */
    Path mapredLocalDir;

    public TaskControllerPathDeletionContext(FileSystem fs, Path mapredLocalDir,
                                             TaskController taskController,
                                             String user) {
      super(fs, null);
      this.taskController = taskController;
      this.mapredLocalDir = mapredLocalDir;
      this.user = user;
    }

    @Override
    protected String getPathForCleanup() {
      if (fullPath == null) {
        fullPath = buildPathForDeletion();
      }
      return fullPath;
    }

    /**
     * Return the component of the path under the {@link #mapredLocalDir} to be 
     * cleaned up. Its the responsibility of the class that extends 
     * {@link TaskControllerPathDeletionContext} to provide the correct 
     * component. For example 
     *  - For task related cleanups, either the task-work-dir or task-local-dir
     *    might be returned depending on jvm reuse.
     *  - For job related cleanup, simply the job-local-dir might be returned.
     */
    abstract protected String getPath();
    
    /**
     * Builds the path of taskAttemptDir OR taskWorkDir based on
     * mapredLocalDir, jobId, taskId, etc
     */
    String buildPathForDeletion() {
      return mapredLocalDir.toUri().getPath() + Path.SEPARATOR + getPath();
    }
  }

  /** Contains info related to the path of the file/dir to be deleted. This info
   * is needed by task-controller to build the full path of the task-work-dir or
   * task-local-dir depending on whether the jvm is reused or not.
   */
  static class TaskControllerTaskPathDeletionContext 
  extends TaskControllerPathDeletionContext {
    final Task task;
    final boolean isWorkDir;
    
    public TaskControllerTaskPathDeletionContext(FileSystem fs, 
        Path mapredLocalDir, Task task, boolean isWorkDir, 
        TaskController taskController) {
      super(fs, mapredLocalDir, taskController, task.getUser());
      this.task = task;
      this.isWorkDir = isWorkDir;
    }
    
    /**
     * Returns the taskWorkDir or taskLocalDir based on whether 
     * {@link TaskControllerTaskPathDeletionContext} is configured to delete
     * the workDir.
     */
    @Override
    protected String getPath() {
      String subDir = (isWorkDir) ? TaskTracker.getTaskWorkDir(task.getUser(),
          task.getJobID().toString(), task.getTaskID().toString(),
          task.isTaskCleanupTask())
        : TaskTracker.getLocalTaskDir(task.getUser(),
          task.getJobID().toString(), task.getTaskID().toString(),
          task.isTaskCleanupTask());
      return subDir;
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

  /** Contains info related to the path of the file/dir to be deleted. This info
   * is needed by task-controller to build the full path of the job-local-dir.
   */
  static class TaskControllerJobPathDeletionContext 
  extends TaskControllerPathDeletionContext {
    final JobID jobId;
    
    public TaskControllerJobPathDeletionContext(FileSystem fs, 
        Path mapredLocalDir, JobID id, String user, 
        TaskController taskController) {
      super(fs, mapredLocalDir, taskController, user);
      this.jobId = id;
    }
    
    /**
     * Returns the jobLocalDir of the job to be cleaned up.
     */
    @Override
    protected String getPath() {
      return TaskTracker.getLocalJobDir(user, jobId.toString());
    }
    
    /**
     * Makes the path(and its sub-directories recursively) fully deletable by
     * setting proper permissions(770) by task-controller
     */
    @Override
    protected void enablePathForCleanup() throws IOException {
      getPathForCleanup();// allow init of fullPath, if not inited already
      if (fs.exists(new Path(fullPath))) {
        taskController.enableJobForCleanup(this);
      }
    }
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
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
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
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
  
  static class DebugScriptContext extends TaskExecContext {
    List<String> args;
    File workDir;
    File stdout;
  }

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
   * Sends a QUIT signal to direct the task JVM (and sub-processes) to
   * dump their stack to stdout.
   *
   * @param context task context.
   */
  abstract void dumpTaskStack(TaskControllerContext context);

  /**
   * Initialize user on this TaskTracer in a TaskController specific manner.
   * 
   * @param context
   * @throws IOException
   */
  public abstract void initializeUser(InitializationContext context)
      throws IOException;
  
  /**
   * Launch the task debug script
   * 
   * @param context
   * @throws IOException
   */
  abstract void runDebugScript(DebugScriptContext context) 
      throws IOException;
  
  /**
   * Enable the task for cleanup by changing permissions of the path
   * @param context   path deletion context
   * @throws IOException
   */
  abstract void enableTaskForCleanup(PathDeletionContext context)
      throws IOException;
  
  /**
   * Enable the job for cleanup by changing permissions of the path
   * @param context   path deletion context
   * @throws IOException
   */
  abstract void enableJobForCleanup(PathDeletionContext context)
    throws IOException;
}

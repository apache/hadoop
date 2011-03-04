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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * A {@link TaskController} that runs the task JVMs as the user 
 * who submits the job.
 * 
 * This class executes a setuid executable to implement methods
 * of the {@link TaskController}, including launching the task 
 * JVM and killing it when needed, and also initializing and
 * finalizing the task environment. 
 * <p> The setuid executable is launched using the command line:</p>
 * <p>task-controller user-name command command-args, where</p>
 * <p>user-name is the name of the owner who submits the job</p>
 * <p>command is one of the cardinal value of the 
 * {@link LinuxTaskController.TaskCommands} enumeration</p>
 * <p>command-args depends on the command being launched.</p>
 * 
 * In addition to running and killing tasks, the class also 
 * sets up appropriate access for the directories and files 
 * that will be used by the tasks. 
 */
class LinuxTaskController extends TaskController {

  private static final Log LOG = 
            LogFactory.getLog(LinuxTaskController.class);

  // Name of the executable script that will contain the child
  // JVM command line. See writeCommand for details.
  private static final String COMMAND_FILE = "taskjvm.sh";
  
  // Path to the setuid executable.
  private static String taskControllerExe;
  
  static {
    // the task-controller is expected to be under the $HADOOP_HOME/bin
    // directory.
    File hadoopBin = new File(System.getenv("HADOOP_HOME"), "bin");
    taskControllerExe = 
        new File(hadoopBin, "task-controller").getAbsolutePath();
  }
  
  // The list of directory paths specified in the
  // variable mapred.local.dir. This is used to determine
  // which among the list of directories is picked up
  // for storing data for a particular task.
  private String[] mapredLocalDirs;
  
  // permissions to set on files and directories created.
  // When localized files are handled securely, this string
  // will change to something more restrictive. Until then,
  // it opens up the permissions for all, so that the tasktracker
  // and job owners can access files together.
  private static final String FILE_PERMISSIONS = "ugo+rwx";
  
  // permissions to set on components of the path leading to
  // localized files and directories. Read and execute permissions
  // are required for different users to be able to access the
  // files.
  private static final String PATH_PERMISSIONS = "go+rx";
  
  public LinuxTaskController() {
    super();
  }
  
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    mapredLocalDirs = conf.getStrings("mapred.local.dir");
    //Setting of the permissions of the local directory is done in 
    //setup()
  }
  
  /**
   * List of commands that the setuid script will execute.
   */
  enum TaskCommands {
    LAUNCH_TASK_JVM,
    KILL_TASK_JVM
  }
  
  /**
   * Launch a task JVM that will run as the owner of the job.
   * 
   * This method launches a task JVM by executing a setuid
   * executable that will switch to the user and run the
   * task.
   */
  @Override
  void launchTaskJVM(TaskController.TaskControllerContext context) 
                                        throws IOException {
    JvmEnv env = context.env;
    // get the JVM command line.
    String cmdLine = 
      TaskLog.buildCommandLine(env.setup, env.vargs, env.stdout, env.stderr,
          env.logSize, env.pidFile);
    // write the command to a file in the
    // task specific cache directory
    writeCommand(cmdLine, getTaskCacheDirectory(context));
    
    // Call the taskcontroller with the right parameters.
    List<String> launchTaskJVMArgs = buildTaskCommandArgs(context);
    ShellCommandExecutor shExec =  buildTaskControllerExecutor(
                                    TaskCommands.LAUNCH_TASK_JVM, 
                                    context.task.getUser(),
                                    launchTaskJVMArgs, env);
    context.shExec = shExec;
    shExec.execute();
    LOG.debug("output after executing task jvm = " + shExec.getOutput());
  }

  // convenience API for building command arguments for specific commands
  private List<String> buildTaskCommandArgs(TaskControllerContext context) {
    List<String> commandArgs = new ArrayList<String>(3);
    String taskId = context.task.getTaskID().toString();
    String jobId = getJobId(context);
    commandArgs.add(jobId);
    if(!context.task.isTaskCleanupTask()) {
      commandArgs.add(taskId);
    }else {
      commandArgs.add(taskId + TaskTracker.TASK_CLEANUP_SUFFIX);
    }
    
    LOG.debug("getting the task directory as: " 
                + getTaskCacheDirectory(context));
    commandArgs.add(getDirectoryChosenForTask(
                              new File(getTaskCacheDirectory(context)), 
                              context));
    return commandArgs;
  }
  
  // get the Job ID from the information in the TaskControllerContext
  private String getJobId(TaskControllerContext context) {
    String taskId = context.task.getTaskID().toString();
    TaskAttemptID tId = TaskAttemptID.forName(taskId);
    String jobId = tId.getJobID().toString();
    return jobId;
  }

  // Get the directory from the list of directories configured
  // in mapred.local.dir chosen for storing data pertaining to
  // this task.
  private String getDirectoryChosenForTask(File directory,
                                            TaskControllerContext context) {
    String jobId = getJobId(context);
    String taskId = context.task.getTaskID().toString();
    for (String dir : mapredLocalDirs) {
      File mapredDir = new File(dir);
      File taskDir = new File(mapredDir, TaskTracker.getLocalTaskDir(
          jobId, taskId, context.task.isTaskCleanupTask()));
      if (directory.equals(taskDir)) {
        return dir;
      }
    }
    
    LOG.error("Couldn't parse task cache directory correctly");
    throw new IllegalArgumentException("invalid task cache directory "
                + directory.getAbsolutePath());
  }
  
  /**
   * Kill a launched task JVM running as the user of the job.
   * 
   * This method will launch the task controller setuid executable
   * that in turn will kill the task JVM by sending a kill signal.
   */
  void killTaskJVM(TaskControllerContext context) {
   
    if(context.task == null) {
      LOG.info("Context task null not killing the JVM");
      return;
    }
    
    JvmEnv env = context.env;
    List<String> killTaskJVMArgs = buildTaskCommandArgs(context);
    try {
      ShellCommandExecutor shExec = buildTaskControllerExecutor(
                                      TaskCommands.KILL_TASK_JVM,
                                      context.task.getUser(),
                                      killTaskJVMArgs, 
                                      context.env);
      shExec.execute();
      LOG.debug("Command output :" +shExec.getOutput());
    } catch (IOException ioe) {
      LOG.warn("IOException in killing task: " + ioe.getMessage());
    }
  }

  /**
   * Setup appropriate permissions for directories and files that
   * are used by the task.
   * 
   * As the LinuxTaskController launches tasks as a user, different
   * from the daemon, all directories and files that are potentially 
   * used by the tasks are setup with appropriate permissions that
   * will allow access.
   * 
   * Until secure data handling is implemented (see HADOOP-4491 and
   * HADOOP-4493, for e.g.), the permissions are set up to allow
   * read, write and execute access for everyone. This will be 
   * changed to restricted access as data is handled securely.
   */
  void initializeTask(TaskControllerContext context) {
    // Setup permissions for the job and task cache directories.
    setupTaskCacheFileAccess(context);
    // setup permissions for task log directory
    setupTaskLogFileAccess(context);    
  }
  
  // Allows access for the task to create log files under 
  // the task log directory
  private void setupTaskLogFileAccess(TaskControllerContext context) {
    TaskAttemptID taskId = context.task.getTaskID();
    File f = TaskLog.getTaskLogFile(taskId, TaskLog.LogName.SYSLOG);
    String taskAttemptLogDir = f.getParentFile().getAbsolutePath();
    changeDirectoryPermissions(taskAttemptLogDir, FILE_PERMISSIONS, false);
  }

  // Allows access for the task to read, write and execute 
  // the files under the job and task cache directories
  private void setupTaskCacheFileAccess(TaskControllerContext context) {
    String taskId = context.task.getTaskID().toString();
    JobID jobId = JobID.forName(getJobId(context));
    //Change permission for the task across all the disks
    for(String localDir : mapredLocalDirs) {
      File f = new File(localDir);
      File taskCacheDir = new File(f,TaskTracker.getLocalTaskDir(
          jobId.toString(), taskId, context.task.isTaskCleanupTask()));
      if(taskCacheDir.exists()) {
        changeDirectoryPermissions(taskCacheDir.getPath(), 
            FILE_PERMISSIONS, true);
      }          
    }//end of local directory Iteration 
  }

  // convenience method to execute chmod.
  private void changeDirectoryPermissions(String dir, String mode, 
                                              boolean isRecursive) {
    int ret = 0;
    try {
      ret = FileUtil.chmod(dir, mode, isRecursive);
    } catch (Exception e) {
      LOG.warn("Exception in changing permissions for directory " + dir + 
                  ". Exception: " + e.getMessage());
    }
    if (ret != 0) {
      LOG.warn("Could not change permissions for directory " + dir);
    }
  }
  
  // convenience API to create the executor for launching the
  // setuid script.
  private ShellCommandExecutor buildTaskControllerExecutor(TaskCommands command, 
                                          String userName, 
                                          List<String> cmdArgs, JvmEnv env) 
                                    throws IOException {
    String[] taskControllerCmd = new String[3 + cmdArgs.size()];
    taskControllerCmd[0] = taskControllerExe;
    taskControllerCmd[1] = userName;
    taskControllerCmd[2] = String.valueOf(command.ordinal());
    int i = 3;
    for (String cmdArg : cmdArgs) {
      taskControllerCmd[i++] = cmdArg;
    }
    if (LOG.isDebugEnabled()) {
      for (String cmd : taskControllerCmd) {
        LOG.debug("taskctrl command = " + cmd);
      }
    }
    ShellCommandExecutor shExec = null;
    if(env.workDir != null && env.workDir.exists()) {
      shExec = new ShellCommandExecutor(taskControllerCmd,
          env.workDir, env.env);
    } else {
      shExec = new ShellCommandExecutor(taskControllerCmd);
    }
    
    return shExec;
  }
  
  // Return the task specific directory under the cache.
  private String getTaskCacheDirectory(TaskControllerContext context) {
    // In the case of JVM reuse, the task specific directory
    // is different from what is set with respect with
    // env.workDir. Hence building this from the taskId everytime.
    String taskId = context.task.getTaskID().toString();
    File cacheDirForJob = context.env.workDir.getParentFile().getParentFile();
    if(context.task.isTaskCleanupTask()) {
      taskId = taskId + TaskTracker.TASK_CLEANUP_SUFFIX;
    }
    return new File(cacheDirForJob, taskId).getAbsolutePath(); 
  }
  
  // Write the JVM command line to a file under the specified directory
  // Note that the JVM will be launched using a setuid executable, and
  // could potentially contain strings defined by a user. Hence, to
  // prevent special character attacks, we write the command line to
  // a file and execute it.
  private void writeCommand(String cmdLine, 
                                      String directory) throws IOException {
    
    PrintWriter pw = null;
    String commandFile = directory + File.separator + COMMAND_FILE;
    LOG.info("Writing commands to " + commandFile);
    try {
      FileWriter fw = new FileWriter(commandFile);
      BufferedWriter bw = new BufferedWriter(fw);
      pw = new PrintWriter(bw);
      pw.write(cmdLine);
    } catch (IOException ioe) {
      LOG.error("Caught IOException while writing JVM command line to file. "
                + ioe.getMessage());
    } finally {
      if (pw != null) {
        pw.close();
      }
      // set execute permissions for all on the file.
      File f = new File(commandFile);
      if (f.exists()) {
        f.setReadable(true, false);
        f.setExecutable(true, false);
      }
    }
  }
  

  /**
   * Sets up the permissions of the following directories:
   * 
   * Job cache directory
   * Archive directory
   * Hadoop log directories
   * 
   */
  @Override
  void setup() {
    //set up job cache directory and associated permissions
    String localDirs[] = this.mapredLocalDirs;
    for(String localDir : localDirs) {
      //Cache root
      File cacheDirectory = new File(localDir,TaskTracker.getCacheSubdir());
      File jobCacheDirectory = new File(localDir,TaskTracker.getJobCacheSubdir());
      if(!cacheDirectory.exists()) {
        if(!cacheDirectory.mkdirs()) {
          LOG.warn("Unable to create cache directory : " + 
              cacheDirectory.getPath());
        }
      }
      if(!jobCacheDirectory.exists()) {
        if(!jobCacheDirectory.mkdirs()) {
          LOG.warn("Unable to create job cache directory : " + 
              jobCacheDirectory.getPath());
        }
      }
      //Give world writable permission for every directory under
      //mapred-local-dir.
      //Child tries to write files under it when executing.
      changeDirectoryPermissions(localDir, FILE_PERMISSIONS, true);
    }//end of local directory manipulations
    //setting up perms for user logs
    File taskLog = TaskLog.getUserLogDir();
    changeDirectoryPermissions(taskLog.getPath(), FILE_PERMISSIONS,false);
  }

  /*
   * Create Job directories across disks and set their permissions to 777
   * This way when tasks are run we just need to setup permissions for
   * task folder.
   */
  @Override
  void initializeJob(JobID jobid) {
    for(String localDir : this.mapredLocalDirs) {
      File jobDirectory = new File(localDir, 
          TaskTracker.getLocalJobDir(jobid.toString()));
      if(!jobDirectory.exists()) {
        if(!jobDirectory.mkdir()) {
          LOG.warn("Unable to create job cache directory : " 
              + jobDirectory.getPath());
          continue;
        }
      }
      //Should be recursive because the jar and work folders might be 
      //present under the job cache directory
      changeDirectoryPermissions(
          jobDirectory.getPath(), FILE_PERMISSIONS, true);
    }
  }
  
}


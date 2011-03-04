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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.util.StringUtils;
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
  
  public LinuxTaskController() {
    super();
  }
  
  /**
   * List of commands that the setuid script will execute.
   */
  enum TaskCommands {
    INITIALIZE_USER,
    INITIALIZE_JOB,
    INITIALIZE_DISTRIBUTEDCACHE,
    LAUNCH_TASK_JVM,
    INITIALIZE_TASK,
    TERMINATE_TASK_JVM,
    KILL_TASK_JVM,
  }

  /**
   * Launch a task JVM that will run as the owner of the job.
   * 
   * This method launches a task JVM by executing a setuid executable that will
   * switch to the user and run the task. Also does initialization of the first
   * task in the same setuid process launch.
   */
  @Override
  void launchTaskJVM(TaskController.TaskControllerContext context) 
                                        throws IOException {
    JvmEnv env = context.env;
    // get the JVM command line.
    String cmdLine = 
      TaskLog.buildCommandLine(env.setup, env.vargs, env.stdout, env.stderr,
          env.logSize, true);

    StringBuffer sb = new StringBuffer();
    //export out all the environment variable before child command as
    //the setuid/setgid binaries would not be getting, any environmental
    //variables which begin with LD_*.
    for(Entry<String, String> entry : env.env.entrySet()) {
      sb.append("export ");
      sb.append(entry.getKey());
      sb.append("=");
      sb.append(entry.getValue());
      sb.append("\n");
    }
    sb.append(cmdLine);
    // write the command to a file in the
    // task specific cache directory
    writeCommand(sb.toString(), getTaskCacheDirectory(context));
    
    // Call the taskcontroller with the right parameters.
    List<String> launchTaskJVMArgs = buildLaunchTaskArgs(context);
    ShellCommandExecutor shExec =  buildTaskControllerExecutor(
                                    TaskCommands.LAUNCH_TASK_JVM, 
                                    env.conf.getUser(),
                                    launchTaskJVMArgs, env.workDir, env.env);
    context.shExec = shExec;
    try {
      shExec.execute();
    } catch (Exception e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from task is : " + exitCode);
      // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the task was
      // terminated/killed forcefully. In all other cases, log the
      // task-controller output
      if (exitCode != 143 && exitCode != 137) {
        LOG.warn("Exception thrown while launching task JVM : "
            + StringUtils.stringifyException(e));
        LOG.info("Output from LinuxTaskController's launchTaskJVM follows:");
        logOutput(shExec.getOutput());
      }
      throw new IOException(e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.info("Output from LinuxTaskController's launchTaskJVM follows:");
      logOutput(shExec.getOutput());
    }
  }

  /**
   * Helper method that runs a LinuxTaskController command
   * 
   * @param taskCommand
   * @param user
   * @param cmdArgs
   * @param env
   * @throws IOException
   */
  private void runCommand(TaskCommands taskCommand, String user,
      List<String> cmdArgs, File workDir, Map<String, String> env)
      throws IOException {

    ShellCommandExecutor shExec =
        buildTaskControllerExecutor(taskCommand, user, cmdArgs, workDir, env);
    try {
      shExec.execute();
    } catch (Exception e) {
      LOG.warn("Exit code from " + taskCommand.toString() + " is : "
          + shExec.getExitCode());
      LOG.warn("Exception thrown by " + taskCommand.toString() + " : "
          + StringUtils.stringifyException(e));
      LOG.info("Output from LinuxTaskController's " + taskCommand.toString()
          + " follows:");
      logOutput(shExec.getOutput());
      throw new IOException(e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.info("Output from LinuxTaskController's " + taskCommand.toString()
          + " follows:");
      logOutput(shExec.getOutput());
    }
  }

  /**
   * Returns list of arguments to be passed while initializing a new task. See
   * {@code buildTaskControllerExecutor(TaskCommands, String, List<String>,
   * JvmEnv)} documentation.
   * 
   * @param context
   * @return Argument to be used while launching Task VM
   */
  private List<String> buildInitializeTaskArgs(TaskControllerContext context) {
    List<String> commandArgs = new ArrayList<String>(3);
    String taskId = context.task.getTaskID().toString();
    String jobId = getJobId(context);
    commandArgs.add(jobId);
    if (!context.task.isTaskCleanupTask()) {
      commandArgs.add(taskId);
    } else {
      commandArgs.add(taskId + TaskTracker.TASK_CLEANUP_SUFFIX);
    }
    return commandArgs;
  }

  @Override
  void initializeTask(TaskControllerContext context)
      throws IOException {
    LOG.debug("Going to do " + TaskCommands.INITIALIZE_TASK.toString()
        + " for " + context.task.getTaskID().toString());
    runCommand(TaskCommands.INITIALIZE_TASK, context.env.conf.getUser(),
        buildInitializeTaskArgs(context), context.env.workDir, context.env.env);
  }

  private void logOutput(String output) {
    String shExecOutput = output;
    if (shExecOutput != null) {
      for (String str : shExecOutput.split("\n")) {
        LOG.info(str);
      }
    }
  }

  private String getJobId(TaskControllerContext context) {
    String taskId = context.task.getTaskID().toString();
    TaskAttemptID tId = TaskAttemptID.forName(taskId);
    String jobId = tId.getJobID().toString();
    return jobId;
  }

  /**
   * Returns list of arguments to be passed while launching task VM.
   * See {@code buildTaskControllerExecutor(TaskCommands, 
   * String, List<String>, JvmEnv)} documentation.
   * @param context
   * @return Argument to be used while launching Task VM
   */
  private List<String> buildLaunchTaskArgs(TaskControllerContext context) {
    List<String> commandArgs = new ArrayList<String>(3);
    LOG.debug("getting the task directory as: " 
        + getTaskCacheDirectory(context));
    LOG.debug("getting the tt_root as " +getDirectoryChosenForTask(
        new File(getTaskCacheDirectory(context)), 
        context) );
    commandArgs.add(getDirectoryChosenForTask(
        new File(getTaskCacheDirectory(context)), 
        context));
    commandArgs.addAll(buildInitializeTaskArgs(context));
    return commandArgs;
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
      File taskDir =
          new File(mapredDir, TaskTracker.getTaskWorkDir(context.task
              .getUser(), jobId, taskId, context.task.isTaskCleanupTask()))
              .getParentFile();
      if (directory.equals(taskDir)) {
        return dir;
      }
    }
    
    LOG.error("Couldn't parse task cache directory correctly");
    throw new IllegalArgumentException("invalid task cache directory "
                + directory.getAbsolutePath());
  }

  @Override
  public void initializeDistributedCache(InitializationContext context)
      throws IOException {
    LOG.debug("Going to initialize distributed cache for " + context.user
        + " on the TT");
    runCommand(TaskCommands.INITIALIZE_DISTRIBUTEDCACHE, context.user,
        new ArrayList<String>(), context.workDir, null);
  }

  @Override
  public void initializeUser(InitializationContext context)
      throws IOException {
    LOG.debug("Going to initialize user directories for " + context.user
        + " on the TT");
    runCommand(TaskCommands.INITIALIZE_USER, context.user,
        new ArrayList<String>(), context.workDir, null);
  }

  /**
   * Builds the command line for launching/terminating/killing task JVM.
   * Following is the format for launching/terminating/killing task JVM
   * <br/>
   * For launching following is command line argument:
   * <br/>
   * {@code user-name command tt-root job_id task_id} 
   * <br/>
   * For terminating/killing task jvm.
   * {@code user-name command tt-root task-pid}
   * 
   * @param command command to be executed.
   * @param userName user name
   * @param cmdArgs list of extra arguments
   * @param workDir working directory for the task-controller
   * @param env JVM environment variables.
   * @return {@link ShellCommandExecutor}
   * @throws IOException
   */
  private ShellCommandExecutor buildTaskControllerExecutor(
      TaskCommands command, String userName, List<String> cmdArgs,
      File workDir, Map<String, String> env)
      throws IOException {
    String[] taskControllerCmd = new String[3 + cmdArgs.size()];
    taskControllerCmd[0] = getTaskControllerExecutablePath();
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
    if(workDir != null && workDir.exists()) {
      shExec = new ShellCommandExecutor(taskControllerCmd,
          workDir, env);
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

  protected String getTaskControllerExecutablePath() {
    return taskControllerExe;
  }  

  private List<String> buildInitializeJobCommandArgs(
      JobInitializationContext context) {
    List<String> initJobCmdArgs = new ArrayList<String>();
    initJobCmdArgs.add(context.jobid.toString());
    return initJobCmdArgs;
  }

  @Override
  void initializeJob(JobInitializationContext context)
      throws IOException {
    LOG.debug("Going to initialize job " + context.jobid.toString()
        + " on the TT");
    runCommand(TaskCommands.INITIALIZE_JOB, context.user,
        buildInitializeJobCommandArgs(context), context.workDir, null);
  }
  
  /**
   * API which builds the command line to be pass to LinuxTaskController
   * binary to terminate/kill the task. See 
   * {@code buildTaskControllerExecutor(TaskCommands, 
   * String, List<String>, JvmEnv)} documentation.
   * 
   * 
   * @param context context of task which has to be passed kill signal.
   * 
   */
  private List<String> buildKillTaskCommandArgs(TaskControllerContext 
      context){
    List<String> killTaskJVMArgs = new ArrayList<String>();
    killTaskJVMArgs.add(context.pid);
    return killTaskJVMArgs;
  }
  
  /**
   * Convenience method used to sending appropriate Kill signal to the task 
   * VM
   * @param context
   * @param command
   * @throws IOException
   */
  private void finishTask(TaskControllerContext context,
      TaskCommands command) throws IOException{
    if(context.task == null) {
      LOG.info("Context task null not killing the JVM");
      return;
    }
    ShellCommandExecutor shExec = buildTaskControllerExecutor(
        command, context.env.conf.getUser(), 
        buildKillTaskCommandArgs(context), context.env.workDir, context.env.env);
    try {
      shExec.execute();
    } catch (Exception e) {
      LOG.warn("Output from task-contoller is : " + shExec.getOutput());
      throw new IOException(e);
    }
  }
  
  @Override
  void terminateTask(TaskControllerContext context) {
    try {
      finishTask(context, TaskCommands.TERMINATE_TASK_JVM);
    } catch (Exception e) {
      LOG.warn("Exception thrown while sending kill to the Task VM " + 
          StringUtils.stringifyException(e));
    }
  }
  
  @Override
  void killTask(TaskControllerContext context) {
    try {
      finishTask(context, TaskCommands.KILL_TASK_JVM);
    } catch (Exception e) {
      LOG.warn("Exception thrown while sending destroy to the Task VM " + 
          StringUtils.stringifyException(e));
    }
  }
}


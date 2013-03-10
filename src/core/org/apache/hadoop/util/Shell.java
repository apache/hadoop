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
package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/** 
 * A base class for running a Unix command.
 * 
 * <code>Shell</code> can be used to run unix commands like <code>du</code> or
 * <code>df</code>. It also offers facilities to gate commands by 
 * time-intervals.
 */
abstract public class Shell {
  
  public static final Log LOG = LogFactory.getLog(Shell.class);

  private static boolean IS_JAVA7_OR_ABOVE =
      System.getProperty("java.version").substring(0, 3).compareTo("1.7") >= 0;

  public static boolean isJava7OrAbove() {
    return IS_JAVA7_OR_ABOVE;
  }


  /** Centralized logic to discover and validate the sanity of the Hadoop 
   *  home directory. Returns either NULL or a directory that exists and 
   *  was specified via either -Dhadoop.home.dir or the HADOOP_HOME ENV 
   *  variable.  This does a lot of work so it should only be called 
   *  privately for initialization once per process.
   **/
  private static String checkHadoopHome() {

    // first check the Dflag hadoop.home.dir with JVM scope
    String home = System.getProperty("hadoop.home.dir");

    // fall back to the system/user-global env variable
    if (home == null) {
      home = System.getenv("HADOOP_HOME");
    }

    try {
       // couldn't find either setting for hadoop's home directory
       if (home == null) {
         throw new IOException("HADOOP_HOME or hadoop.home.dir are not set.");
       }

       if (home.startsWith("\"") && home.endsWith("\"")) {
         home = home.substring(1, home.length()-1);
       }

       // check that the home setting is actually a directory that exists
       File homedir = new File(home);
       if (!homedir.isAbsolute() || !homedir.exists() || !homedir.isDirectory()) {
         throw new IOException("Hadoop home directory " + homedir
           + " does not exist, is not a directory, or is not an absolute path.");
       }

       home = homedir.getCanonicalPath();

    } catch (IOException ioe) {
       LOG.error("Failed to detect a valid hadoop home directory", ioe);
       home = null;
    }
    
    return home;
  }
  private static String HADOOP_HOME_DIR = checkHadoopHome();

  // Public getter, throws an exception if HADOOP_HOME failed validation
  // checks and is being referenced downstream.
  public static final String getHadoopHome() throws IOException {
    if (HADOOP_HOME_DIR == null) {
      throw new IOException("Misconfigured HADOOP_HOME cannot be referenced.");
    }

    return HADOOP_HOME_DIR;
  }

  /** fully qualify the path to a binary that should be in a known hadoop 
   *  bin location. This is primarily useful for disambiguating call-outs 
   *  to executable sub-components of Hadoop to avoid clashes with other 
   *  executables that may be in the path.  Caveat:  this call doesn't 
   *  just format the path to the bin directory.  It also checks for file 
   *  existence of the composed path. The output of this call should be 
   *  cached by callers.
   * */
  public static final String getQualifiedBinPath(String executable) 
  throws IOException {
    // construct hadoop bin path to the specified executable
    String fullExeName = HADOOP_HOME_DIR + File.separator + "bin" 
      + File.separator + executable;

    File exeFile = new File(fullExeName);
    if (!exeFile.exists()) {
      throw new IOException("Could not locate executable " + fullExeName
        + " in the Hadoop binaries.");
    }

    return exeFile.getCanonicalPath();
  }

  /** Set to true on Windows platforms */
  public static final boolean WINDOWS
                = System.getProperty("os.name").startsWith("Windows");
  
  public static final boolean LINUX
                = System.getProperty("os.name").startsWith("Linux");

  /* Set flag for aiding Windows porting temporarily for branch-1-win*/
  // TODO - this needs to be fixed
  public static final boolean DISABLEWINDOWS_TEMPORARILY = WINDOWS; 
  
  /** a Windows utility to emulate Unix commands */
  public static final String WINUTILS = getWinUtilsPath();

  public static final String getWinUtilsPath() {
    String winUtilsPath = null;

    try {
      if (WINDOWS) {
        winUtilsPath = getQualifiedBinPath("winutils.exe");
      }
    } catch (IOException ioe) {
       LOG.error("Failed to locate the winutils binary in the hadoop binary path",
         ioe);
    }

    return winUtilsPath;
  }

  /** a Unix command to get the current user's name */
  public final static String USER_NAME_COMMAND = "whoami";

  /** Windows CreateProcess synchronization object */
  public static final Object WindowsProcessLaunchLock = new Object();

  /** a Unix command to set the change user's groups list */
  public static final String SET_GROUP_COMMAND = "chgrp";

  /** Return a command to get the current user's groups list */
  public static String[] getGroupsCommand() {
    return (WINDOWS)? new String[]{"cmd", "/c", "groups"}
                    : new String[]{"bash", "-c", "groups"};
  }

  /** Return a command to get a given user's groups list */
  public static String[] getGroupsForUserCommand(final String user) {
    //'groups username' command return is non-consistent across different unixes
    return (WINDOWS)? new String[] { WINUTILS, "groups", "-F", "\"" + user + "\""}
                    : new String [] {"bash", "-c", "id -Gn " + user};
  }

  /** Return a command to get a given netgroup's user list */
  public static String[] getUsersForNetgroupCommand(final String netgroup) {
    //'groups username' command return is non-consistent across different unixes
    return (WINDOWS)? new String [] {"cmd", "/c", "getent netgroup " + netgroup}
                    : new String [] {"bash", "-c", "getent netgroup " + netgroup};
  }

  /** Return a command to get permission information. */
  public static String[] getGetPermissionCommand() {
    return (WINDOWS) ? new String[] { WINUTILS, "ls", "-F" }
                     : new String[] { "/bin/ls", "-ld" };
  }

  /** Return a command to set permission */
  public static String[] getSetPermissionCommand(String perm, boolean recursive) {
    if (recursive) {
      return (WINDOWS) ? new String[] { WINUTILS, "chmod", "-R", perm }
                         : new String[] { "chmod", "-R", perm };
    } else {
      return (WINDOWS) ? new String[] { WINUTILS, "chmod", perm }
                       : new String[] { "chmod", perm };
    }
  }

  /**
   * Return a command to set permission for specific file.
   * 
   * @param perm String permission to set
   * @param recursive boolean true to apply to all sub-directories recursively
   * @param file String file to set
   * @return String[] containing command and arguments
   */
  public static String[] getSetPermissionCommand(String perm, boolean recursive,
                                                 String file) {
    String[] baseCmd = getSetPermissionCommand(perm, recursive);
    String[] cmdWithFile = Arrays.copyOf(baseCmd, baseCmd.length + 1);
    cmdWithFile[cmdWithFile.length - 1] = file;
    return cmdWithFile;
  }

  /** Return a regular expression string that match environment variables */
  public static String getEnvironmentVariableRegex() {
    return (WINDOWS) ? "%([A-Za-z_][A-Za-z0-9_]*?)%" :
      "\\$([A-Za-z_][A-Za-z0-9_]*)";
  }

  /** Return a command to set owner */
  public static String[] getSetOwnerCommand(String owner) {
    return (WINDOWS) ? new String[] { WINUTILS, "chown", "\"" + owner + "\"" }
                     : new String[] { "chown", owner };
  }
  
  /** Return a command to create symbolic links */
  public static String[] getSymlinkCommand(String target, String link) {
    return WINDOWS ? new String[] { WINUTILS, "symlink", link, target }
                   : new String[] { "ln", "-s", target, link };
  }
  
  /** Return a command to execute the given command in OS shell.
   *  On Windows, the passed in groupId can be used to launch
   *  and associate the given groupId in a process group. On
   *  non-Windows, groupId is ignored. */
  public static String[] getRunCommand(String command,
                                       String groupId) {
    if (WINDOWS) {
      if(ProcessTree.isSetsidAvailable) {
        return new String[] { Shell.WINUTILS, "task", "create",
                              groupId, "cmd /c " + command };
      } else {
        return new String[] { "cmd", "/c", command };
      }
    } else {
      return new String[] { "bash", "-c", command };
    }
  }

  /** Return a command to send a kill signal to a given groupId */
  public static String[] getSignalKillProcessGroupCommand(int code,
                                                          String groupId) {
    if (WINDOWS) {
      return new String[] { Shell.WINUTILS, "task", "kill", groupId };
    } else {
      return new String[] { "kill", "-" + code , "-" + groupId };
    }
  }

  /**Time after which the executing script would be timedout*/
  protected long timeOutInterval = 0L;
  /** If or not script timed out*/
  private AtomicBoolean timedOut;

  /** a Unix command to get ulimit of a process. */
  public static final String ULIMIT_COMMAND = "ulimit";
  
  /** 
   * Get the Unix command for setting the maximum virtual memory available
   * to a given child process. This is only relevant when we are forking a
   * process from within the Mapper or the Reducer implementations.
   * Also see Hadoop Pipes and Hadoop Streaming.
   * 
   * It also checks to ensure that we are running on a *nix platform else 
   * (e.g. in Cygwin/Windows) it returns <code>null</code>.
   * @param memoryLimit virtual memory limit
   * @return a <code>String[]</code> with the ulimit command arguments or 
   *         <code>null</code> if we are running on a non *nix platform or
   *         if the limit is unspecified.
   */
  public static String[] getUlimitMemoryCommand(int memoryLimit) {
    // ulimit isn't supported on Windows
    if (WINDOWS) {
      return null;
    }
    
    return new String[] {ULIMIT_COMMAND, "-v", String.valueOf(memoryLimit)};
  }
  
  /** 
   * Get the Unix command for setting the maximum virtual memory available
   * to a given child process. This is only relevant when we are forking a
   * process from within the {@link org.apache.hadoop.mapred.Mapper} or the 
   * {@link org.apache.hadoop.mapred.Reducer} implementations 
   * e.g. <a href="{@docRoot}/org/apache/hadoop/mapred/pipes/package-summary.html">Hadoop Pipes</a> 
   * or <a href="{@docRoot}/org/apache/hadoop/streaming/package-summary.html">Hadoop Streaming</a>.
   * 
   * It also checks to ensure that we are running on a *nix platform else 
   * (e.g. in Cygwin/Windows) it returns <code>null</code>.
   * @param conf configuration
   * @return a <code>String[]</code> with the ulimit command arguments or 
   *         <code>null</code> if we are running on a non *nix platform or
   *         if the limit is unspecified.
   * @deprecated Use {@link #getUlimitMemoryCommand(int)}
   */
  @Deprecated
  public static String[] getUlimitMemoryCommand(Configuration conf) {
    // ulimit isn't supported on Windows
    if (WINDOWS) {
      return null;
    }
    
    // get the memory limit from the configuration
    String ulimit = conf.get("mapred.child.ulimit");
    if (ulimit == null) {
      return null;
    }
    
    // Parse it to ensure it is legal/sane
    int memoryLimit = Integer.valueOf(ulimit);
    
    return getUlimitMemoryCommand(memoryLimit);
  }

  /** Token separator regex used to parse Shell tool outputs */
  public static final String TOKEN_SEPARATOR_REGEX
                = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";

  private long    interval;   // refresh interval in msec
  private long    lastTime;   // last time the command was performed
  private Map<String, String> environment; // env for the command execution
  private File dir;
  private Process process; // sub process used to execute the command
  private int exitCode;

  /**If or not script finished executing*/
  private volatile AtomicBoolean completed;
  
  public Shell() {
    this(0L);
  }
  
  /**
   * @param interval the minimum duration to wait before re-executing the 
   *        command.
   */
  public Shell( long interval ) {
    this.interval = interval;
    this.lastTime = (interval<0) ? 0 : -interval;
  }
  
  /** set the environment for the command 
   * @param env Mapping of environment variables
   */
  protected void setEnvironment(Map<String, String> env) {
    this.environment = env;
  }

  /** set the working directory 
   * @param dir The directory where the command would be executed
   */
  protected void setWorkingDirectory(File dir) {
    this.dir = dir;
  }

  /** check to see if a command needs to be executed and execute if needed */
  protected void run() throws IOException {
    if (lastTime + interval > System.currentTimeMillis())
      return;
    exitCode = 0; // reset for next run
    runCommand();
  }

  /** Run a command */
  private void runCommand() throws IOException { 
    ProcessBuilder builder = new ProcessBuilder(getExecString());
    Timer timeOutTimer = null;
    ShellTimeoutTimerTask timeoutTimerTask = null;
    timedOut = new AtomicBoolean(false);
    completed = new AtomicBoolean(false);
    
    if (environment != null) {
      builder.environment().putAll(this.environment);
    }
    if (dir != null) {
      builder.directory(this.dir);
    }
    
    if (Shell.WINDOWS) {
      synchronized (WindowsProcessLaunchLock) {
        // To workaround the race condition issue with child processes
        // inheriting unintended handles during process launch that can
        // lead to hangs on reading output and error streams, we
        // serialize process creation. More info available at:
        // http://support.microsoft.com/kb/315939
        process = builder.start();
      }
    } else {
      process = builder.start();
    }

    if (timeOutInterval > 0) {
      timeOutTimer = new Timer();
      timeoutTimerTask = new ShellTimeoutTimerTask(
          this);
      //One time scheduling.
      timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
    }
    final BufferedReader errReader = 
            new BufferedReader(new InputStreamReader(process
                                                     .getErrorStream()));
    BufferedReader inReader = 
            new BufferedReader(new InputStreamReader(process
                                                     .getInputStream()));
    final StringBuffer errMsg = new StringBuffer();
    
    // read error and input streams as this would free up the buffers
    // free the error stream buffer
    Thread errThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            errMsg.append(line);
            errMsg.append(System.getProperty("line.separator"));
            line = errReader.readLine();
          }
        } catch(IOException ioe) {
          LOG.warn("Error reading the error stream", ioe);
        }
      }
    };
    try {
      errThread.start();
    } catch (IllegalStateException ise) { }
    try {
      parseExecResult(inReader); // parse the output
      // clear the input stream buffer
      String line = inReader.readLine();
      while(line != null) { 
        line = inReader.readLine();
      }
      // wait for the process to finish and check the exit code
      exitCode  = process.waitFor();
      try {
        // make sure that the error thread exits
        errThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted while reading the error stream", ie);
      }
      completed.set(true);
      //the timeout thread handling
      //taken care in finally block
      if (exitCode != 0) {
        throw new ExitCodeException(exitCode, errMsg.toString());
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie.toString());
    } finally {
      if ((timeOutTimer!=null) && !timedOut.get()) {
        timeOutTimer.cancel();
      }
      // close the input stream
      try {
        inReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      if (!completed.get()) {
        errThread.interrupt();
      }
      try {
        errReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the error stream", ioe);
      }
      process.destroy();
      lastTime = System.currentTimeMillis();
    }
  }

  /** return an array containing the command name & its parameters */ 
  protected abstract String[] getExecString();
  
  /** Parse the execution result */
  protected abstract void parseExecResult(BufferedReader lines)
  throws IOException;

  /** get the current sub-process executing the given command 
   * @return process executing the command
   */
  public Process getProcess() {
    return process;
  }

  /** get the exit code 
   * @return the exit code of the process
   */
  public int getExitCode() {
    return exitCode;
  }

  /**
   * This is an IOException with exit code added.
   */
  public static class ExitCodeException extends IOException {
    int exitCode;
    
    public ExitCodeException(int exitCode, String message) {
      super(message);
      this.exitCode = exitCode;
    }
    
    public int getExitCode() {
      return exitCode;
    }
  }
  
  /**
   * A simple shell command executor.
   * 
   * <code>ShellCommandExecutor</code>should be used in cases where the output 
   * of the command needs no explicit parsing and where the command, working 
   * directory and the environment remains unchanged. The output of the command 
   * is stored as-is and is expected to be small.
   */
  public static class ShellCommandExecutor extends Shell {
    
    private String[] command;
    private StringBuffer output;
    
    
    public ShellCommandExecutor(String[] execString) {
      this(execString, null);
    }
    
    public ShellCommandExecutor(String[] execString, File dir) {
      this(execString, dir, null);
    }
   
    public ShellCommandExecutor(String[] execString, File dir, 
                                 Map<String, String> env) {
      this(execString, dir, env , 0L);
    }

    /**
     * Create a new instance of the ShellCommandExecutor to execute a command.
     * 
     * @param execString The command to execute with arguments
     * @param dir If not-null, specifies the directory which should be set
     *            as the current working directory for the command.
     *            If null, the current working directory is not modified.
     * @param env If not-null, environment of the command will include the
     *            key-value pairs specified in the map. If null, the current
     *            environment is not modified.
     * @param timeout Specifies the time in milliseconds, after which the
     *                command will be killed and the status marked as timedout.
     *                If 0, the command will not be timed out. 
     */
    public ShellCommandExecutor(String[] execString, File dir, 
        Map<String, String> env, long timeout) {
      command = execString.clone();
      if (dir != null) {
        setWorkingDirectory(dir);
      }
      if (env != null) {
        setEnvironment(env);
      }
      timeOutInterval = timeout;
    }

    /** Execute the shell command. */
    public void execute() throws IOException {
      this.run();    
    }

    protected String[] getExecString() {
      return command;
    }

    protected void parseExecResult(BufferedReader lines) throws IOException {
      output = new StringBuffer();
      char[] buf = new char[512];
      int nRead;
      while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
        output.append(buf, 0, nRead);
      }
    }
    
    /** Get the output of the shell command.*/
    public String getOutput() {
      return (output == null) ? "" : output.toString();
    }

    /**
     * Returns the commands of this instance.
     * Arguments with spaces in are presented with quotes round; other
     * arguments are presented raw
     *
     * @return a string representation of the object.
     */
    public String toString() {
      StringBuilder builder = new StringBuilder();
      String[] args = getExecString();
      for (String s : args) {
        if (s.indexOf(' ') >= 0) {
          builder.append('"').append(s).append('"');
        } else {
          builder.append(s);
        }
        builder.append(' ');
      }
      return builder.toString();
    }
  }
  
  /**
   * To check if the passed script to shell command executor timed out or
   * not.
   * 
   * @return if the script timed out.
   */
  public boolean isTimedOut() {
    return timedOut.get();
  }
  
  /**
   * Set if the command has timed out.
   * 
   */
  private void setTimedOut() {
    this.timedOut.set(true);
  }
  
  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static String execCommand(String ... cmd) throws IOException {
    return execCommand(null, cmd, 0L);
  }
  
  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @param timeout time in milliseconds after which script should be marked timeout
   * @return the output of the executed command.o
   */
  
  public static String execCommand(Map<String, String> env, String[] cmd,
      long timeout) throws IOException {
    ShellCommandExecutor exec = new ShellCommandExecutor(cmd, null, env, 
                                                          timeout);
    exec.execute();
    return exec.getOutput();
  }

  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static String execCommand(Map<String,String> env, String ... cmd) 
      throws IOException {
    return execCommand(env, cmd, 0L);
  }
  
  /**
   * Timer which is used to timeout scripts spawned off by shell.
   */
  private static class ShellTimeoutTimerTask extends TimerTask {

    private Shell shell;

    public ShellTimeoutTimerTask(Shell shell) {
      this.shell = shell;
    }

    @Override
    public void run() {
      Process p = shell.getProcess();
      try {
        p.exitValue();
      } catch (Exception e) {
        //Process has not terminated.
        //So check if it has completed 
        //if not just destroy it.
        if (p != null && !shell.completed.get()) {
          shell.setTimedOut();
          p.destroy();
        }
      }
    }
  }
}

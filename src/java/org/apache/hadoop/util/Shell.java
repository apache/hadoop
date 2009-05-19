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
import java.util.Map;

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
  
  /** a Unix command to get the current user's name */
  public final static String USER_NAME_COMMAND = "whoami";
  /** a Unix command to get the current user's groups list */
  public static String[] getGROUPS_COMMAND() {
    return new String[]{"bash", "-c", "groups"};
  }
  /** a Unix command to set permission */
  public static final String SET_PERMISSION_COMMAND = "chmod";
  /** a Unix command to set owner */
  public static final String SET_OWNER_COMMAND = "chown";
  public static final String SET_GROUP_COMMAND = "chgrp";
  /** Return a Unix command to get permission information. */
  public static String[] getGET_PERMISSION_COMMAND() {
    //force /bin/ls, except on windows.
    return new String[] {(WINDOWS ? "ls" : "/bin/ls"), "-ld"};
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
   */
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

    return new String[] {"ulimit", "-v", String.valueOf(memoryLimit)};
  }
  
  /** Set to true on Windows platforms */
  public static final boolean WINDOWS /* borrowed from Path.WINDOWS */
                = System.getProperty("os.name").startsWith("Windows");
  
  private long    interval;   // refresh interval in msec
  private long    lastTime;   // last time the command was performed
  private Map<String, String> environment; // env for the command execution
  private File dir;
  private Process process; // sub process used to execute the command
  private int exitCode;
  
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
    boolean completed = false;
    
    if (environment != null) {
      builder.environment().putAll(this.environment);
    }
    if (dir != null) {
      builder.directory(this.dir);
    }
    
    process = builder.start();
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
      exitCode = process.waitFor();
      try {
        // make sure that the error thread exits
        errThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted while reading the error stream", ie);
      }
      completed = true;
      if (exitCode != 0) {
        throw new ExitCodeException(exitCode, errMsg.toString());
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie.toString());
    } finally {
      // close the input stream
      try {
        inReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      if (!completed) {
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
      command = execString.clone();
    }

    public ShellCommandExecutor(String[] execString, File dir) {
      this(execString);
      this.setWorkingDirectory(dir);
    }

    public ShellCommandExecutor(String[] execString, File dir, 
                                 Map<String, String> env) {
      this(execString, dir);
      this.setEnvironment(env);
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
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static String execCommand(String ... cmd) throws IOException {
    return execCommand(null, cmd);
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
    ShellCommandExecutor exec = new ShellCommandExecutor(cmd);
    if (env != null) {
      exec.setEnvironment(env);
    }
    exec.execute();
    return exec.getOutput();
  }
}

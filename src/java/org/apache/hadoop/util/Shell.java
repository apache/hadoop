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

import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
  /** Return a Unix command to get permission information. */
  public static String[] getGET_PERMISSION_COMMAND() {
    return new String[]{"ls", "-ld"};
  }

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
            errMsg.append(System.getProperty("line.seperator"));
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
      if (exitCode != 0) {
        if (errMsg.length() == 0) {
          errMsg.append("Command exit with status code " + exitCode);
        }
        throw new IOException(errMsg.toString());
      }
      completed = true;
    } catch (InterruptedException ie) {
      throw new IOException(ie.toString());
    } finally {
      // close the input stream
      try {
        inReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      if (completed) {
        try {
          // make sure that the error thread exits
          errThread.join();
        } catch (InterruptedException ie) {
          LOG.warn("Interrupted while reading the error stream", ie);
        }
      } else {
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
  }
  
  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static String execCommand(String ... cmd) throws IOException {
    ShellCommandExecutor exec = new ShellCommandExecutor(cmd);
    exec.execute();
    return exec.getOutput();
  }
}

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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

/** A base class for running a unix command like du or df*/
abstract public class ShellCommand {
  /** a Unix command to get the current user's name */
  public final static String USER_NAME_COMMAND = "whoami";
  /** a Unix command to get the current user's groups list */
  public static final String GROUPS_COMMAND = "groups";
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
  
  ShellCommand() {
    this(0L);
  }
  
  ShellCommand( long interval ) {
    this.interval = interval;
    this.lastTime = (interval<0) ? 0 : -interval;
  }
  
  /** check to see if a command needs to be execuated */
  protected void run() throws IOException {
    if (lastTime + interval > System.currentTimeMillis())
      return;
    runCommand();
  }

  /** Run a command */
  private void runCommand() throws IOException { 
    Process process;
    process = Runtime.getRuntime().exec(getExecString());

    try {
      if (process.waitFor() != 0) {
        throw new IOException
          (new BufferedReader(new InputStreamReader(process.getErrorStream()))
           .readLine());
      }
      parseExecResult(new BufferedReader(
          new InputStreamReader(process.getInputStream())));
    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      process.destroy();
      lastTime = System.currentTimeMillis();
    }
  }

  /** return an array comtaining the command name & its parameters */ 
  protected abstract String[] getExecString();
  
  /** Parse the execution result */
  protected abstract void parseExecResult(BufferedReader lines)
  throws IOException;

  /// A simple implementation of Command
  private static class SimpleCommandExecutor extends ShellCommand {
    
    private String[] command;
    private StringBuffer reply;
    
    SimpleCommandExecutor(String[] execString) {
      command = execString;
    }

    @Override
    protected String[] getExecString() {
      return command;
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      reply = new StringBuffer();
      char[] buf = new char[512];
      int nRead;
      while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
        reply.append(buf, 0, nRead);
      }
    }
    
    String getReply() {
      return (reply == null) ? "" : reply.toString();
    }
  }
  
  /** 
   * Static method to execute a command. Covers most of the simple cases 
   * without requiring the user to implement Command interface.
   */
  public static String execCommand(String ... cmd) throws IOException {
    SimpleCommandExecutor exec = new SimpleCommandExecutor(cmd);
    exec.run();
    return exec.getReply();
  }
}

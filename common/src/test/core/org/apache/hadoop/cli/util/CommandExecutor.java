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

package org.apache.hadoop.cli.util;

import org.apache.hadoop.cli.CLITestHelper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.StringTokenizer;

/**
 *
 * This class execute commands and captures the output
 */
public abstract class CommandExecutor {  
  protected String[] getCommandAsArgs(final String cmd, final String masterKey,
		                                       final String master) {
    StringTokenizer tokenizer = new StringTokenizer(cmd, " ");
    String[] args = new String[tokenizer.countTokens()];
    
    int i = 0;
    while (tokenizer.hasMoreTokens()) {
      args[i] = tokenizer.nextToken();

      args[i] = args[i].replaceAll(masterKey, master);
      args[i] = args[i].replaceAll("CLITEST_DATA", 
        new File(CLITestHelper.TEST_CACHE_DATA_DIR).
        toURI().toString().replace(' ', '+'));
      args[i] = args[i].replaceAll("USERNAME", System.getProperty("user.name"));

      i++;
    }
    
    return args;
  }
  
  public Result executeCommand(final String cmd) throws Exception {
    int exitCode = 0;
    Exception lastException = null;
    
    
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    PrintStream origOut = System.out;
    PrintStream origErr = System.err;
    
    System.setOut(new PrintStream(bao));
    System.setErr(new PrintStream(bao));
    
    try {
      execute(cmd);
    } catch (Exception e) {
      e.printStackTrace();
      lastException = e;
      exitCode = -1;
    } finally {
      System.setOut(origOut);
      System.setErr(origErr);
    }
    return new Result(bao.toString(), exitCode, lastException, cmd);
  }
  
  protected abstract void execute(final String cmd) throws Exception;
  
  public static class Result {
    final String commandOutput;
    final int exitCode;
    final Exception exception;
    final String cmdExecuted;
    public Result(String commandOutput, int exitCode, Exception exception,
        String cmdExecuted) {
      this.commandOutput = commandOutput;
      this.exitCode = exitCode;
      this.exception = exception;
      this.cmdExecuted = cmdExecuted;
    }
    
    public String getCommandOutput() {
      return commandOutput;
    }

    public int getExitCode() {
      return exitCode;
    }

    public Exception getException() {
      return exception;
    }

    public String getCommand() {
      return cmdExecuted;
    }
  }

}

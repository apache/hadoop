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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.cli.TestCLI;

/**
 *
 * This class executed commands and captures the output
 */
public class CommandExecutor {
  private static String commandOutput = null;
  private static int exitCode = 0;
  private static Exception lastException = null;
  private static String cmdExecuted = null;
  
  private static String[] getFSCommandAsArgs(final String cmd, 
		  final String namenode) {
    StringTokenizer tokenizer = new StringTokenizer(cmd, " ");
    String[] args = new String[tokenizer.countTokens()];
    
    int i = 0;
    while (tokenizer.hasMoreTokens()) {
      args[i] = tokenizer.nextToken();

      args[i] = args[i].replaceAll("NAMENODE", namenode);
      args[i] = args[i].replaceAll("CLITEST_DATA", 
        new File(TestCLI.TEST_CACHE_DATA_DIR).
        toURI().toString().replace(' ', '+'));
      args[i] = args[i].replaceAll("USERNAME", System.getProperty("user.name"));

      i++;
    }
    
    return args;
  }
  
  public static int executeFSCommand(final String cmd, final String namenode) {
    exitCode = 0;
    
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    PrintStream origOut = System.out;
    PrintStream origErr = System.err;
    
    System.setOut(new PrintStream(bao));
    System.setErr(new PrintStream(bao));
    
    FsShell shell = new FsShell();
    String[] args = getFSCommandAsArgs(cmd, namenode);
    cmdExecuted = cmd;
    
    try {
      ToolRunner.run(shell, args);
    } catch (Exception e) {
      e.printStackTrace();
      lastException = e;
      exitCode = -1;
    } finally {
      System.setOut(origOut);
      System.setErr(origErr);
    }
    
    commandOutput = bao.toString();
    
    return exitCode;
  }
  
  public static String getLastCommandOutput() {
    return commandOutput;
  }

  public static int getLastExitCode() {
    return exitCode;
  }

  public static Exception getLastException() {
    return lastException;
  }

  public static String getLastCommand() {
    return cmdExecuted;
  }
}

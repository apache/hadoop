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

import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 *  Class to execute a shell.
 *  @deprecated Use {@link ShellCommandExecutor} instead.
 */
public class ShellUtil {
  
  ShellCommandExecutor shexec; // shell to execute a command
  
    /**
     * @param args list containing command and command line arguments
     * @param dir Current working directory
     * @param env Environment for the command
     */
    public ShellUtil (List<String> args, File dir, Map<String, String> env) {
      shexec = new ShellCommandExecutor(args.toArray(new String[0]), dir, 
                                         env);
    }
	
    /**
     * Executes the command.
     * @throws IOException
     * @throws InterruptedException
     */
    public void execute() throws IOException {
      // start the process and wait for it to execute
      shexec.execute();
    }
    /**
     * @return process
     */
    public Process getProcess() {
      return shexec.getProcess();
    }

    /**
     * @return exit-code of the process
     */
    public int getExitCode() {
      return shexec.getExitCode();
   }
}

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

import org.apache.hadoop.fs.FsShell;

/**
 * Class to define Test Command along with its type
 */
public class CLITestCmd implements CLICommand {
  private final CLICommandTypes type;
  private final String cmd;

  public CLITestCmd(String str, CLICommandTypes type) {
    cmd = str;
    this.type = type;
  }

  @Override
  public CommandExecutor getExecutor(String tag) throws IllegalArgumentException {
    if (getType() instanceof CLICommandFS)
      return new FSCmdExecutor(tag, new FsShell());
    throw new
        IllegalArgumentException("Unknown type of test command: " + getType());
  }

  @Override
  public CLICommandTypes getType() {
    return type;
  }
  
  @Override
  public String getCmd() {
    return cmd;
  }
  
  @Override
  public String toString() {
    return cmd;
  }
}

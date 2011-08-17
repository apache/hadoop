/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.cli;

import org.apache.hadoop.cli.util.CLICommandTypes;
import org.apache.hadoop.cli.util.CLITestCmd;
import org.apache.hadoop.cli.util.CommandExecutor;

public class CLITestCmdMR extends CLITestCmd {
  public CLITestCmdMR(String str, CLICommandTypes type) {
    super(str, type);
  }

  /**
   * This is not implemented because HadoopArchive constructor requires JobConf
   * to create an archive object. Because TestMRCLI uses setup method from
   * TestHDFSCLI the initialization of executor objects happens before a config
   * is created and updated. Thus, actual calls to executors happen in the body
   * of the test method.
   */
  @Override
  public CommandExecutor getExecutor(String tag)
      throws IllegalArgumentException {
    throw new IllegalArgumentException("Method isn't supported");
  }
}

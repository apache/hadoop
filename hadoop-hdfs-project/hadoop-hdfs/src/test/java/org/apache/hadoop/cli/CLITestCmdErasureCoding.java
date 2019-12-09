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

import org.apache.hadoop.cli.util.CLICommandErasureCodingCli;
import org.apache.hadoop.cli.util.CLICommandTypes;
import org.apache.hadoop.cli.util.CLITestCmd;
import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.cli.util.ErasureCodingCliCmdExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.ECAdmin;

public class CLITestCmdErasureCoding extends CLITestCmd {
  public CLITestCmdErasureCoding(String str, CLICommandTypes type) {
    super(str, type);
  }

  @Override
  public CommandExecutor getExecutor(String tag, Configuration conf) throws IllegalArgumentException {
    if (getType() instanceof CLICommandErasureCodingCli)
      return new ErasureCodingCliCmdExecutor(tag, new ECAdmin(conf));
    return super.getExecutor(tag, conf);
  }
}

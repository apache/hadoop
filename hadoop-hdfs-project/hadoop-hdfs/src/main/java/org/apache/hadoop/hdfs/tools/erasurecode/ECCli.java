/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.tools.erasurecode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * CLI for the erasure code encoding operations.
 */
public class ECCli extends FsShell {

  private final static String usagePrefix =
      "Usage: hdfs erasurecode [generic options]";

  @Override
  protected String getUsagePrefix() {
    return usagePrefix;
  }

  @Override
  protected void registerCommands(CommandFactory factory) {
    factory.registerCommands(ECCommand.class);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new HdfsConfiguration();
    int res = ToolRunner.run(conf, new ECCli(), args);
    System.exit(res);
  }
}
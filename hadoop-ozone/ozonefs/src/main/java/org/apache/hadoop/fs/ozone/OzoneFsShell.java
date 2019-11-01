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
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.util.ToolRunner;

/** Provide command line access to a Ozone FileSystem. */
@InterfaceAudience.Private
public class OzoneFsShell extends FsShell {

  private final String ozoneUsagePrefix = "Usage: ozone fs [generic options]";

  /**
   * Default ctor with no configuration.  Be sure to invoke
   * {@link #setConf(Configuration)} with a valid configuration prior
   * to running commands.
   */
  public OzoneFsShell() {
    this(null);
  }

  /**
   * Construct a OzoneFsShell with the given configuration.
   *
   * Commands can be executed via {@link #run(String[])}
   * @param conf the hadoop configuration
   */
  public OzoneFsShell(Configuration conf) {
    super(conf);
  }

  protected void registerCommands(CommandFactory factory) {
    // TODO: DFSAdmin subclasses FsShell so need to protect the command
    // registration.  This class should morph into a base class for
    // commands, and then this method can be abstract
    if (this.getClass().equals(OzoneFsShell.class)) {
      factory.registerCommands(FsCommand.class);
    }
  }

  @Override
  protected String getUsagePrefix() {
    return ozoneUsagePrefix;
  }

  /**
   * Main entry point to execute fs commands.
   *
   * @param argv the command and its arguments
   * @throws Exception upon error
   */
  public static void main(String[] argv) throws Exception {
    OzoneFsShell shell = newShellInstance();
    Configuration conf = new Configuration();
    conf.setQuietMode(false);
    shell.setConf(conf);
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }

  // TODO: this should be abstract in a base class
  protected static OzoneFsShell newShellInstance() {
    return new OzoneFsShell();
  }
}

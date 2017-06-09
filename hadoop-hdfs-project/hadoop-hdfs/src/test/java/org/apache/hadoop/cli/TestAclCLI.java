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
package org.apache.hadoop.cli;

import org.apache.hadoop.cli.util.CLICommand;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAclCLI extends CLITestHelperDFS {
  private MiniDFSCluster cluster = null;
  private FileSystem fs = null;
  private String namenode = null;
  private String username = null;

  protected void initConf() {
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_POSIX_ACL_INHERITANCE_ENABLED_KEY, false);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    initConf();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    fs = cluster.getFileSystem();
    namenode = conf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");
    username = System.getProperty("user.name");
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected String getTestFile() {
    return "testAclCLI.xml";
  }

  @Override
  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", namenode);
    expCmd = expCmd.replaceAll("USERNAME", username);
    expCmd = expCmd.replaceAll("#LF#",
        System.getProperty("line.separator"));
    expCmd = super.expandCommand(expCmd);
    return expCmd;
  }

  @Override
  protected Result execute(CLICommand cmd) throws Exception {
    return cmd.getExecutor(namenode, conf).executeCommand(cmd.getCmd());
  }

  @Test
  @Override
  public void testAll() {
    super.testAll();
  }
}

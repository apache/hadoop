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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.cli.util.CLICommand;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDeleteCLI extends CLITestHelperDFS {
  protected MiniDFSCluster dfsCluster = null;
  protected FileSystem fs = null;
  protected String namenode = null;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setLong(CommonConfigurationKeysPublic.
        HADOOP_SHELL_SAFELY_DELETE_LIMIT_NUM_FILES, 5);

    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    dfsCluster.waitClusterUp();
    namenode = conf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");

    fs = dfsCluster.getFileSystem();
    assertTrue("Not an HDFS: " + fs.getUri(),
        fs instanceof DistributedFileSystem);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    Thread.sleep(2000);
    super.tearDown();
  }

  @Override
  protected String getTestFile() {
    return "testDeleteConf.xml";
  }

  @Override
  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", namenode);
    expCmd = super.expandCommand(expCmd);
    return expCmd;
  }

  @Override
  protected Result execute(CLICommand cmd) throws Exception {
    return cmd.getExecutor(namenode, conf).executeCommand(cmd.getCmd());
  }

  @Test
  @Override
  public void testAll () {
    super.testAll();
  }
}
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

import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.cli.util.CLITestData.TestCmd;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.ToolRunner;

public class TestHDFSCLI extends TestCLI{

  protected MiniDFSCluster dfsCluster = null;
  protected DistributedFileSystem dfs = null;
  protected String namenode = null;
  protected DFSAdminCmdExecutor dfsAdmCmdExecutor = null;
  protected FSCmdExecutor fsCmdExecutor = null;
  
  public void setUp() throws Exception {
    super.setUp();
    conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
        HDFSPolicyProvider.class, PolicyProvider.class);
    
    // Many of the tests expect a replication value of 1 in the output
    conf.setInt("dfs.replication", 1);
    
    // Build racks and hosts configuration to test dfsAdmin -printTopology
    String [] racks =  {"/rack1", "/rack1", "/rack2", "/rack2",
                        "/rack2", "/rack3", "/rack4", "/rack4" };
    String [] hosts = {"host1", "host2", "host3", "host4",
                       "host5", "host6", "host7", "host8" };
    dfsCluster = new MiniDFSCluster(conf, 8, true, racks, hosts);
    
    namenode = conf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");
    
    username = System.getProperty("user.name");
    dfsAdmCmdExecutor = new DFSAdminCmdExecutor(namenode);
    fsCmdExecutor =  new FSCmdExecutor(namenode);

    FileSystem fs = dfsCluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    dfs = (DistributedFileSystem) fs;
  }

  protected String getTestFile() {
    return "testHDFSConf.xml";
  }
  
  public void tearDown() throws Exception {
    dfs.close();
    dfsCluster.shutdown();
    Thread.sleep(2000);
    super.tearDown();
  }

  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", namenode);
    expCmd = super.expandCommand(cmd);
    return expCmd;
  }
  
  protected Result execute(TestCmd cmd) throws Exception {
    CommandExecutor executor = null;
    switch(cmd.getType()) {
    case DFSADMIN:
      executor = dfsAdmCmdExecutor;
      break;
    case FS:
      executor = fsCmdExecutor;
      break;
    default:
      throw new Exception("Unknow type of Test command:"+ cmd.getType());
    }
    return executor.executeCommand(cmd.getCmd());
  }
  
  public static class DFSAdminCmdExecutor extends CommandExecutor {
    private String namenode = null;
    public DFSAdminCmdExecutor(String namenode) {
      this.namenode = namenode;
    }
    
    protected void execute(final String cmd) throws Exception{
      DFSAdmin shell = new DFSAdmin();
      String[] args = getCommandAsArgs(cmd, "NAMENODE", this.namenode);
      ToolRunner.run(shell, args);
    }
  }
  
  public static class FSCmdExecutor extends CommandExecutor {
    private String namenode = null;
    public FSCmdExecutor(String namenode) {
      this.namenode = namenode;
    }
    protected void execute(final String cmd) throws Exception{
      FsShell shell = new FsShell();
      String[] args = getCommandAsArgs(cmd, "NAMENODE", this.namenode);
      ToolRunner.run(shell, args);
    }
  }
}

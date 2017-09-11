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
import org.apache.hadoop.cli.util.CLICommandErasureCodingCli;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.xml.sax.SAXException;

public class TestErasureCodingCLI extends CLITestHelper {
  private final int NUM_OF_DATANODES = 3;
  private MiniDFSCluster dfsCluster = null;
  private FileSystem fs = null;
  private String namenode = null;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    conf.set(DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_ENABLED_KEY,
        "RS-6-3-1024k,RS-3-2-1024k,XOR-2-1-1024k");

    dfsCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_OF_DATANODES).build();
    dfsCluster.waitClusterUp();
    namenode = conf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");

    username = System.getProperty("user.name");

    fs = dfsCluster.getFileSystem();
  }

  @Override
  protected String getTestFile() {
    return "testErasureCodingConf.xml";
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
  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", namenode);
    expCmd = expCmd.replaceAll("#LF#", System.getProperty("line.separator"));
    expCmd = super.expandCommand(expCmd);
    return expCmd;
  }

  @Override
  protected TestConfigFileParser getConfigParser() {
    return new TestErasureCodingAdmin();
  }

  private class TestErasureCodingAdmin extends
      CLITestHelper.TestConfigFileParser {
    @Override
    public void endElement(String uri, String localName, String qName)
        throws SAXException {
      if (qName.equals("ec-admin-command")) {
        if (testCommands != null) {
          testCommands.add(new CLITestCmdErasureCoding(charString,
              new CLICommandErasureCodingCli()));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new CLITestCmdErasureCoding(charString,
              new CLICommandErasureCodingCli()));
        }
      } else {
        super.endElement(uri, localName, qName);
      }
    }
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

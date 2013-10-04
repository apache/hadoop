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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cli.util.CLICommand;
import org.apache.hadoop.cli.util.CLICommandCacheAdmin;
import org.apache.hadoop.cli.util.CLICommandTypes;
import org.apache.hadoop.cli.util.CLITestCmd;
import org.apache.hadoop.cli.util.CacheAdminCmdExecutor;
import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.CacheAdmin;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TestCacheAdminCLI extends CLITestHelper {

  public static final Log LOG = LogFactory.getLog(TestCacheAdminCLI.class);

  protected MiniDFSCluster dfsCluster = null;
  protected FileSystem fs = null;
  protected String namenode = null;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
        HDFSPolicyProvider.class, PolicyProvider.class);

    // Many of the tests expect a replication value of 1 in the output
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();

    dfsCluster.waitClusterUp();
    namenode = conf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");
    username = System.getProperty("user.name");

    fs = dfsCluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    Thread.sleep(2000);
    super.tearDown();
  }

  @Override
  protected String getTestFile() {
    return "testCacheAdminConf.xml";
  }

  @Override
  protected TestConfigFileParser getConfigParser() {
    return new TestConfigFileParserCacheAdmin();
  }

  private class TestConfigFileParserCacheAdmin extends
      CLITestHelper.TestConfigFileParser {
    @Override
    public void endElement(String uri, String localName, String qName)
        throws SAXException {
      if (qName.equals("cache-admin-command")) {
        if (testCommands != null) {
          testCommands.add(new CLITestCmdCacheAdmin(charString,
              new CLICommandCacheAdmin()));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new CLITestCmdCacheAdmin(charString,
              new CLICommandCacheAdmin()));
        }
      } else {
        super.endElement(uri, localName, qName);
      }
    }
  }

  private class CLITestCmdCacheAdmin extends CLITestCmd {

    public CLITestCmdCacheAdmin(String str, CLICommandTypes type) {
      super(str, type);
    }

    @Override
    public CommandExecutor getExecutor(String tag)
        throws IllegalArgumentException {
      if (getType() instanceof CLICommandCacheAdmin) {
        return new CacheAdminCmdExecutor(tag, new CacheAdmin(conf));
      }
      return super.getExecutor(tag);
    }
  }

  @Override
  protected Result execute(CLICommand cmd) throws Exception {
    return cmd.getExecutor("").executeCommand(cmd.getCmd());
  }

  @Test
  @Override
  public void testAll () {
    super.testAll();
  }
}

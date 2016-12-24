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

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.cli.util.CLICommand;
import org.apache.hadoop.cli.util.CLICommandCryptoAdmin;
import org.apache.hadoop.cli.util.CLICommandTypes;
import org.apache.hadoop.cli.util.CLITestCmd;
import org.apache.hadoop.cli.util.CryptoAdminCmdExecutor;
import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.cli.util.CommandExecutor.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.CryptoAdmin;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TestCryptoAdminCLI extends CLITestHelperDFS {
  protected MiniDFSCluster dfsCluster = null;
  protected FileSystem fs = null;
  protected String namenode = null;
  private static File tmpDir;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
        HDFSPolicyProvider.class, PolicyProvider.class);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 10);

    tmpDir = GenericTestUtils.getTestDir(UUID.randomUUID().toString());
    final Path jksPath = new Path(tmpDir.toString(), "test.jks");
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri());

    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    dfsCluster.waitClusterUp();
    createAKey("mykey", conf);
    namenode = conf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "file:///");

    username = System.getProperty("user.name");

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

  /* Helper function to create a key in the Key Provider. */
  private void createAKey(String keyName, Configuration conf)
    throws NoSuchAlgorithmException, IOException {
    final KeyProvider provider =
        dfsCluster.getNameNode().getNamesystem().getProvider();
    final KeyProvider.Options options = KeyProvider.options(conf);
    provider.createKey(keyName, options);
    provider.flush();
    }

  @Override
  protected String getTestFile() {
    return "testCryptoConf.xml";
  }

  @Override
  protected String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", namenode);
    expCmd = expCmd.replaceAll("#LF#",
        System.getProperty("line.separator"));
    expCmd = super.expandCommand(expCmd);
    return expCmd;
  }

  @Override
  protected TestConfigFileParser getConfigParser() {
    return new TestConfigFileParserCryptoAdmin();
  }

  private class TestConfigFileParserCryptoAdmin extends
      CLITestHelperDFS.TestConfigFileParserDFS {
    @Override
    public void endElement(String uri, String localName, String qName)
        throws SAXException {
      if (qName.equals("crypto-admin-command")) {
        if (testCommands != null) {
          testCommands.add(new CLITestCmdCryptoAdmin(charString,
              new CLICommandCryptoAdmin()));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new CLITestCmdCryptoAdmin(charString,
              new CLICommandCryptoAdmin()));
        }
      } else {
        super.endElement(uri, localName, qName);
      }
    }
  }

  private class CLITestCmdCryptoAdmin extends CLITestCmd {
    public CLITestCmdCryptoAdmin(String str, CLICommandTypes type) {
      super(str, type);
    }

    @Override
    public CommandExecutor getExecutor(String tag, Configuration conf)
        throws IllegalArgumentException {
      if (getType() instanceof CLICommandCryptoAdmin) {
        return new CryptoAdminCmdExecutor(tag, new CryptoAdmin(conf));
      }
      return super.getExecutor(tag, conf);
    }
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

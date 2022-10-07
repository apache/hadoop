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
package org.apache.hadoop.hdfs;

import java.io.File;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestWrapper;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FileSystemTestWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.EncryptionFaultInjector;
import org.apache.hadoop.hdfs.server.namenode.EncryptionZoneManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static org.junit.Assert.*;


public class TestEnclosingRoot {
  static final Logger LOG = LoggerFactory.getLogger(TestEncryptionZones.class);

  protected Configuration conf;
  private FileSystemTestHelper fsHelper;

  protected MiniDFSCluster cluster;
  protected HdfsAdmin dfsAdmin;
  protected DistributedFileSystem fs;
  private File testRootDir;
  protected final String TEST_KEY = "test_key";
  private static final String NS_METRICS = "FSNamesystem";
  private static final String  AUTHORIZATION_EXCEPTION_MESSAGE =
      "User [root] is not authorized to perform [READ] on key " +
          "with ACL name [key2]!!";

  protected FileSystemTestWrapper fsWrapper;
  protected FileContextTestWrapper fcWrapper;

  protected static final EnumSet<CreateEncryptionZoneFlag> NO_TRASH =
      EnumSet.of(CreateEncryptionZoneFlag.NO_TRASH);

  protected String getKeyProviderURI() {
    return JavaKeyStoreProvider.SCHEME_NAME + "://file" +
        new Path(testRootDir.toString(), "test.jks").toUri();
  }

  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    fsHelper = new FileSystemTestHelper();
    // Set up java key store
    String testRoot = fsHelper.getTestRootDir();
    testRootDir = new File(testRoot).getAbsoluteFile();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    // Lower the batch size for testing
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        2);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(EncryptionZoneManager.class), Level.TRACE);
    fs = cluster.getFileSystem();
    fsWrapper = new FileSystemTestWrapper(fs);
    fcWrapper = new FileContextTestWrapper(
        FileContext.getFileContext(cluster.getURI(), conf));
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);
    setProvider();
    // Create a test key
    DFSTestUtil.createKey(TEST_KEY, cluster, conf);
  }

  protected void setProvider() {
    // Need to set the client's KeyProvider to the NN's for JKS,
    // else the updates do not get flushed properly
    fs.getClient().setKeyProvider(cluster.getNameNode().getNamesystem()
        .getProvider());
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    EncryptionFaultInjector.instance = new EncryptionFaultInjector();
  }

  @Test
  public void testBasicOperations() throws Exception {
    final Path rootDir = new Path("/");
    final Path zone1 = new Path(rootDir, "zone1");
    final Path zone1FileDNE = new Path(zone1, "newDNE.txt");

    assertEquals(fs.getEnclosingRoot(rootDir), rootDir);
    assertEquals(fs.getEnclosingRoot(zone1), rootDir);

    fs.mkdirs(zone1);
    dfsAdmin.createEncryptionZone(zone1, TEST_KEY, NO_TRASH);
    assertEquals(fs.getEnclosingRoot(rootDir), rootDir);
    assertEquals(fs.getEnclosingRoot(zone1), zone1);
    assertEquals(fs.getEnclosingRoot(zone1FileDNE), zone1);
  }

}

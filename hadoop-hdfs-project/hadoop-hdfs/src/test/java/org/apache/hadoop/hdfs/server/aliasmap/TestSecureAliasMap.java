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

package org.apache.hadoop.hdfs.server.aliasmap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.TestSecureNNWithQJM;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test DN & NN communication in secured hdfs with alias map.
 */
public class TestSecureAliasMap {
  private static HdfsConfiguration baseConf;
  private static File baseDir;
  private static MiniKdc kdc;

  private static String keystoresDir;
  private static String sslConfDir;
  private MiniDFSCluster cluster;
  private HdfsConfiguration conf;
  private FileSystem fs;

  @BeforeClass
  public static void init() throws Exception {
    baseDir =
        GenericTestUtils.getTestDir(TestSecureAliasMap.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    baseConf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, baseConf);
    UserGroupInformation.setConfiguration(baseConf);
    assertTrue("Expected configuration to enable security",
        UserGroupInformation.isSecurityEnabled());

    String userName = UserGroupInformation.getLoginUser().getShortUserName();
    File keytabFile = new File(baseDir, userName + ".keytab");
    String keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    kdc.createPrincipal(keytabFile, userName + "/" + krbInstance,
        "HTTP/" + krbInstance);

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSecureNNWithQJM.class);
    MiniDFSCluster.setupKerberosConfiguration(baseConf, userName,
        kdc.getRealm(), keytab, keystoresDir, sslConfDir);
  }

  @AfterClass
  public static void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(baseDir);
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @After
  public void shutdown() throws IOException {
    IOUtils.cleanupWithLogger(null, fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testSecureConnectionToAliasMap() throws Exception {
    conf = new HdfsConfiguration(baseConf);
    MiniDFSCluster.setupNamenodeProvidedConfiguration(conf);
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
        "127.0.0.1:" + NetUtils.getFreeSocketPort());

    int numNodes = 1;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numNodes)
        .storageTypes(
            new StorageType[] {StorageType.DISK, StorageType.PROVIDED})
        .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    FSNamesystem namesystem = cluster.getNamesystem();
    BlockManager blockManager = namesystem.getBlockManager();
    DataNode dn = cluster.getDataNodes().get(0);

    FsDatasetSpi.FsVolumeReferences volumes =
        dn.getFSDataset().getFsVolumeReferences();
    FsVolumeSpi providedVolume = null;
    for (FsVolumeSpi volume : volumes) {
      if (volume.getStorageType().equals(StorageType.PROVIDED)) {
        providedVolume = volume;
        break;
      }
    }

    String[] bps = providedVolume.getBlockPoolList();
    assertEquals("Missing provided volume", 1, bps.length);

    BlockAliasMap aliasMap = blockManager.getProvidedStorageMap().getAliasMap();
    BlockAliasMap.Reader reader = aliasMap.getReader(null, bps[0]);
    assertNotNull("Failed to create blockAliasMap reader", reader);
  }
}
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
package org.apache.hadoop.hdfs.qjournal;

import static org.junit.Assert.*;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestSecureNNWithQJM {

  private static final Path TEST_PATH = new Path("/test-dir");
  private static final Path TEST_PATH_2 = new Path("/test-dir-2");
  private static final String PREFIX = "hadoop.http.authentication.";

  private static HdfsConfiguration baseConf;
  private static File baseDir;
  private static String keystoresDir;
  private static String sslConfDir;
  private static MiniKdc kdc;

  private MiniDFSCluster cluster;
  private HdfsConfiguration conf;
  private FileSystem fs;
  private MiniJournalCluster mjc;

  @Rule
  public Timeout timeout = new Timeout(180000);

  @BeforeClass
  public static void init() throws Exception {
    baseDir =
        GenericTestUtils.getTestDir(TestSecureNNWithQJM.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    baseConf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS,
      baseConf);
    UserGroupInformation.setConfiguration(baseConf);
    assertTrue("Expected configuration to enable security",
      UserGroupInformation.isSecurityEnabled());

    String userName = UserGroupInformation.getLoginUser().getShortUserName();
    File keytabFile = new File(baseDir, userName + ".keytab");
    String keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    kdc.createPrincipal(keytabFile,
      userName + "/" + krbInstance,
      "HTTP/" + krbInstance);
    String hdfsPrincipal = userName + "/" + krbInstance + "@" + kdc.getRealm();
    String spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.getRealm();

    baseConf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        AuthenticationFilterInitializer.class.getName());
    baseConf.set(PREFIX + "type", "kerberos");
    baseConf.set(PREFIX + "kerberos.keytab", keytab);
    baseConf.set(PREFIX + "kerberos.principal", spnegoPrincipal);
    baseConf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    baseConf.set(DFS_JOURNALNODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
      spnegoPrincipal);
    baseConf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    baseConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    baseConf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    baseConf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
      TestSecureNNWithQJM.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, baseConf, false);
    baseConf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    baseConf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  @AfterClass
  public static void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(baseDir);
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration(baseConf);
  }

  @After
  public void shutdown() throws IOException {
    IOUtils.cleanupWithLogger(null, fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    if (mjc != null) {
      mjc.shutdown();
      mjc = null;
    }
  }

  @Test
  public void testSecureMode() throws Exception {
    doNNWithQJMTest();
  }

  @Test
  public void testSecondaryNameNodeHttpAddressNotNeeded() throws Exception {
    conf.set(DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "null");
    doNNWithQJMTest();
  }

  /**
   * Tests use of QJM with the defined cluster.
   *
   * @throws IOException if there is an I/O error
   */
  private void doNNWithQJMTest() throws IOException {
    startCluster();
    assertTrue(fs.mkdirs(TEST_PATH));

    // Restart the NN and make sure the edit was persisted
    // and loaded again
    restartNameNode();

    assertTrue(fs.exists(TEST_PATH));
    assertTrue(fs.mkdirs(TEST_PATH_2));

    // Restart the NN again and make sure both edits are persisted.
    restartNameNode();
    assertTrue(fs.exists(TEST_PATH));
    assertTrue(fs.exists(TEST_PATH_2));
  }

  /**
   * Restarts the NameNode and obtains a new FileSystem.
   *
   * @throws IOException if there is an I/O error
   */
  private void restartNameNode() throws IOException {
    IOUtils.cleanupWithLogger(null, fs);
    cluster.restartNameNode();
    fs = cluster.getFileSystem();
  }

  /**
   * Starts a cluster using QJM with the defined configuration.
   *
   * @throws IOException if there is an I/O error
   */
  private void startCluster() throws IOException {
    mjc = new MiniJournalCluster.Builder(conf)
      .build();
    mjc.waitActive();
    conf.set(DFS_NAMENODE_EDITS_DIR_KEY,
      mjc.getQuorumJournalURI("myjournal").toString());
    cluster = new MiniDFSCluster.Builder(conf)
      .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }
}

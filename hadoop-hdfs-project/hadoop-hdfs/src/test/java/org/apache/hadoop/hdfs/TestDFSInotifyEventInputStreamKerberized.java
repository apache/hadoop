/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.qjournal.TestSecureNNWithQJM;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY;
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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class for Kerberized test cases for {@link DFSInotifyEventInputStream}.
 */
public class TestDFSInotifyEventInputStreamKerberized {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDFSInotifyEventInputStreamKerberized.class);
  private static final String PREFIX = "hadoop.http.authentication.";

  private File baseDir;
  private String keystoresDir;
  private String sslConfDir;

  private MiniKdc kdc;
  private Configuration baseConf;
  private Configuration conf;
  private MiniQJMHACluster cluster;
  private File generalHDFSKeytabFile;
  private File nnKeytabFile;

  @Rule
  public Timeout timeout = new Timeout(180000);

  @Test
  public void testWithKerberizedCluster() throws Exception {
    conf = new HdfsConfiguration(baseConf);
    // make sure relogin can happen after tgt expiration.
    conf.setInt(HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 3);
    // make sure the rpc connection is not reused
    conf.setInt(IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY, 100);
    conf.setInt(IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 2000);
    Client.setConnectTimeout(conf, 2000);
    // force the remote journal to be the only edits dir, so we can test
    // EditLogFileInputStream$URLLog behavior.
    cluster = new MiniQJMHACluster.Builder(conf).setForceRemoteEditsOnly(true)
        .build();
    cluster.getDfsCluster().waitActive();
    cluster.getDfsCluster().transitionToActive(0);

    final UserGroupInformation ugi = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI("hdfs",
            generalHDFSKeytabFile.getAbsolutePath());

    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        LOG.info("Current user is: " + UserGroupInformation.getCurrentUser()
            + " login user is:" + UserGroupInformation.getLoginUser());
        Configuration clientConf =
            new Configuration(cluster.getDfsCluster().getConfiguration(0));
        try (DistributedFileSystem clientFs =
            (DistributedFileSystem) FileSystem.get(clientConf)) {
          clientFs.mkdirs(new Path("/test"));
          LOG.info("mkdir /test success");
          final DFSInotifyEventInputStream eis =
              clientFs.getInotifyEventStream();
          // verify we can poll
          EventBatch batch;
          while ((batch = eis.poll()) != null) {
            LOG.info("txid: " + batch.getTxid());
          }
          assertNull("poll should not return anything", eis.poll());

          Thread.sleep(6000);
          LOG.info("Slept 6 seconds to make sure the TGT has expired.");

          UserGroupInformation.getCurrentUser()
              .checkTGTAndReloginFromKeytab();
          clientFs.mkdirs(new Path("/test1"));
          LOG.info("mkdir /test1 success");

          // verify we can poll after a tgt expiration interval
          batch = eis.poll();
          assertNotNull("poll should return something", batch);
          assertEquals(1, batch.getEvents().length);
          assertNull("poll should not return anything", eis.poll());
          return null;
        }
      }
    });
  }

  @Before
  public void initKerberizedCluster() throws Exception {
    baseDir = new File(System.getProperty("test.build.dir", "target/test-dir"),
        TestDFSInotifyEventInputStreamKerberized.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    final Properties kdcConf = MiniKdc.createConf();
    kdcConf.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "5");
    kdcConf.setProperty(MiniKdc.MIN_TICKET_LIFETIME, "5");
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    baseConf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, baseConf);
    UserGroupInformation.setConfiguration(baseConf);
    assertTrue("Expected configuration to enable security",
        UserGroupInformation.isSecurityEnabled());

    final String userName = "hdfs";
    nnKeytabFile = new File(baseDir, userName + ".keytab");
    final String keytab = nnKeytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    final String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";
    kdc.createPrincipal(nnKeytabFile, userName + "/" + krbInstance,
        "HTTP/" + krbInstance);
    generalHDFSKeytabFile = new File(baseDir, "hdfs_general.keytab");
    kdc.createPrincipal(generalHDFSKeytabFile, "hdfs");
    assertTrue(generalHDFSKeytabFile.exists());
    final String hdfsPrincipal =
        userName + "/" + krbInstance + "@" + kdc.getRealm();
    final String spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.getRealm();

    baseConf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        AuthenticationFilterInitializer.class.getName());
    baseConf.set(PREFIX + "type", "kerberos");
    baseConf.set(PREFIX + "kerberos.keytab", nnKeytabFile.getAbsolutePath());
    baseConf.set(PREFIX + "kerberos.principal", "HTTP/" + krbInstance);

    baseConf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    baseConf
        .set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    baseConf.set(DFS_JOURNALNODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        spnegoPrincipal);
    baseConf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    baseConf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    baseConf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSecureNNWithQJM.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, baseConf, false);
    baseConf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    baseConf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  @After
  public void shutdownCluster() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(baseDir);
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }
}

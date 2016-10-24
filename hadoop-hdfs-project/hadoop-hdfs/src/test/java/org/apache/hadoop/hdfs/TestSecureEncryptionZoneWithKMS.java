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

import static org.junit.Assert.assertTrue;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestWrapper;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for HDFS encryption zone without external Kerberos KDC by leveraging
 * Kerby-based MiniKDC, MiniKMS and MiniDFSCluster. This provides additional
 * unit test coverage on Secure(Kerberos) KMS + HDFS.
 */
public class TestSecureEncryptionZoneWithKMS {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestSecureEncryptionZoneWithKMS.class);

  private static final Path TEST_PATH = new Path("/test-dir");
  private static HdfsConfiguration baseConf;
  private static File baseDir;
  private static String keystoresDir;
  private static String sslConfDir;
  private static final EnumSet< CreateEncryptionZoneFlag > NO_TRASH =
      EnumSet.of(CreateEncryptionZoneFlag.NO_TRASH);

  private static final String HDFS_USER_NAME = "hdfs";
  private static final String SPNEGO_USER_NAME = "HTTP";
  private static final String OOZIE_USER_NAME = "oozie";
  private static final String OOZIE_PROXIED_USER_NAME = "oozie_user";

  private static String hdfsPrincipal;
  private static String spnegoPrincipal;
  private static String ooziePrincipal;
  private static String keytab;

  // MiniKDC
  private static MiniKdc kdc;

  // MiniKMS
  private static MiniKMS miniKMS;
  private final String testKey = "test_key";

  // MiniDFS
  private MiniDFSCluster cluster;
  private HdfsConfiguration conf;
  private FileSystem fs;
  private HdfsAdmin dfsAdmin;
  private FileSystemTestWrapper fsWrapper;

  public static File getTestDir() throws Exception {
    File file = new File("dummy");
    file = file.getAbsoluteFile();
    file = file.getParentFile();
    file = new File(file, "target");
    file = new File(file, UUID.randomUUID().toString());
    if (!file.mkdirs()) {
      throw new RuntimeException("Could not create test directory: " + file);
    }
    return file;
  }

  @Rule
  public Timeout timeout = new Timeout(30000);

  @BeforeClass
  public static void init() throws Exception {
    baseDir = getTestDir();
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

    File keytabFile = new File(baseDir, "test.keytab");
    keytab = keytabFile.getAbsolutePath();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";

    kdc.createPrincipal(keytabFile,
        HDFS_USER_NAME + "/" + krbInstance,
        SPNEGO_USER_NAME + "/" + krbInstance,
        OOZIE_USER_NAME + "/" + krbInstance,
        OOZIE_PROXIED_USER_NAME + "/" + krbInstance);

    hdfsPrincipal = HDFS_USER_NAME + "/" + krbInstance + "@" + kdc
        .getRealm();
    spnegoPrincipal = SPNEGO_USER_NAME + "/" + krbInstance + "@" +
        kdc.getRealm();
    ooziePrincipal = OOZIE_USER_NAME + "/" + krbInstance + "@" +
        kdc.getRealm();

    // Allow oozie to proxy user
    baseConf.set("hadoop.proxyuser.oozie.hosts", "*");
    baseConf.set("hadoop.proxyuser.oozie.groups", "*");

    baseConf.set("hadoop.user.group.static.mapping.overrides",
        OOZIE_PROXIED_USER_NAME + "=oozie");

    baseConf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    baseConf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    baseConf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        spnegoPrincipal);
    baseConf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    baseConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    baseConf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    baseConf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.set(DFS_JOURNALNODE_HTTPS_ADDRESS_KEY, "localhost:0");
    baseConf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    // Set a small (2=4*0.5) KMSClient EDEK cache size to trigger
    // on demand refill upon the 3rd file creation
    baseConf.set(KMS_CLIENT_ENC_KEY_CACHE_SIZE, "4");
    baseConf.set(KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK, "0.5");

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestSecureEncryptionZoneWithKMS.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, baseConf, false);
    baseConf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    baseConf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    File kmsFile = new File(baseDir, "kms-site.xml");
    if (kmsFile.exists()) {
      FileUtil.fullyDelete(kmsFile);
    }

    Configuration kmsConf = new Configuration(true);
    kmsConf.set(KMSConfiguration.KEY_PROVIDER_URI,
        "jceks://file@" + new Path(baseDir.toString(), "kms.keystore")
            .toUri());
    kmsConf.set("hadoop.kms.authentication.type", "kerberos");
    kmsConf.set("hadoop.kms.authentication.kerberos.keytab", keytab);
    kmsConf.set("hadoop.kms.authentication.kerberos.principal",
        "HTTP/localhost");
    kmsConf.set("hadoop.kms.authentication.kerberos.name.rules", "DEFAULT");
    kmsConf.set("hadoop.kms.acl.GENERATE_EEK", "hdfs");

    Writer writer = new FileWriter(kmsFile);
    kmsConf.writeXml(writer);
    writer.close();

    // Start MiniKMS
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
    miniKMS = miniKMSBuilder.setKmsConfDir(baseDir).build();
    miniKMS.start();
  }

  @AfterClass
  public static void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
    if (miniKMS != null) {
      miniKMS.stop();
    }
    FileUtil.fullyDelete(baseDir);
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Before
  public void setup() throws Exception {
    // Start MiniDFS Cluster
    baseConf
        .set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
            getKeyProviderURI());
    baseConf.setBoolean(DFSConfigKeys
        .DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);

    conf = new HdfsConfiguration(baseConf);
    cluster = new MiniDFSCluster.Builder(conf)
        .build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    fsWrapper = new FileSystemTestWrapper(fs);
    dfsAdmin = new HdfsAdmin(cluster.getURI(), conf);

    // Wait cluster to be active
    cluster.waitActive();

    // Create a test key
    DFSTestUtil.createKey(testKey, cluster, conf);
  }

  @After
  public void shutdown() throws IOException {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private String getKeyProviderURI() {
    return KMSClientProvider.SCHEME_NAME + "://" +
        miniKMS.getKMSUrl().toExternalForm().replace("://", "@");
  }

  @Test
  public void testSecureEncryptionZoneWithKMS() throws IOException,
      InterruptedException {
    final Path zonePath = new Path(TEST_PATH, "TestEZ1");
    fsWrapper.mkdir(zonePath, FsPermission.getDirDefault(), true);
    fsWrapper.setOwner(zonePath, OOZIE_PROXIED_USER_NAME, "supergroup");
    dfsAdmin.createEncryptionZone(zonePath, testKey, NO_TRASH);

    UserGroupInformation oozieUgi = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(ooziePrincipal, keytab);
    UserGroupInformation proxyUserUgi =
        UserGroupInformation.createProxyUser(OOZIE_PROXIED_USER_NAME,
            oozieUgi);
    proxyUserUgi.doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws IOException {
            // Get a client handler within the proxy user context for createFile
            try (DistributedFileSystem dfs = cluster.getFileSystem()) {
              for (int i = 0; i < 3; i++) {
                Path filePath = new Path(zonePath, "testData." + i + ".dat");
                DFSTestUtil.createFile(dfs, filePath, 1024, (short) 3, 1L);
              }
              return null;
            } catch (IOException e) {
              throw new IOException(e);
            }
          }
        });
  }
}
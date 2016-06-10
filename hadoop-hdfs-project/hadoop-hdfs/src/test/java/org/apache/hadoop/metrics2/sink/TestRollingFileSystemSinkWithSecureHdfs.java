/*
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

package org.apache.hadoop.metrics2.sink;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.NullGroupsMapping;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.assertTrue;

/**
 * Test the {@link RollingFileSystemSink} class in the context of HDFS with
 * Kerberos enabled.
 */
public class TestRollingFileSystemSinkWithSecureHdfs
    extends RollingFileSystemSinkTestBase {
  private static final int  NUM_DATANODES = 4;
  private static MiniKdc kdc;
  private static String sinkPrincipal;
  private static String sinkKeytab;
  private static String hdfsPrincipal;
  private static String hdfsKeytab;
  private static String spnegoPrincipal;
  private MiniDFSCluster cluster = null;
  private UserGroupInformation sink = null;

  /**
   * Setup the KDC for testing a secure HDFS cluster.
   *
   * @throws Exception thrown if the KDC setup fails
   */
  @BeforeClass
  public static void initKdc() throws Exception {
    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, ROOT_TEST_DIR);
    kdc.start();

    File sinkKeytabFile = new File(ROOT_TEST_DIR, "sink.keytab");
    sinkKeytab = sinkKeytabFile.getAbsolutePath();
    kdc.createPrincipal(sinkKeytabFile, "sink/localhost");
    sinkPrincipal = "sink/localhost@" + kdc.getRealm();

    File hdfsKeytabFile = new File(ROOT_TEST_DIR, "hdfs.keytab");
    hdfsKeytab = hdfsKeytabFile.getAbsolutePath();
    kdc.createPrincipal(hdfsKeytabFile, "hdfs/localhost",
        "HTTP/localhost");
    hdfsPrincipal = "hdfs/localhost@" + kdc.getRealm();
    spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();
  }

  /**
   * Setup the mini-DFS cluster.
   *
   * @throws Exception thrown if the cluster setup fails
   */
  @Before
  public void initCluster() throws Exception {
    HdfsConfiguration conf = createSecureConfig("authentication,privacy");

    RollingFileSystemSink.hasFlushed = false;
    RollingFileSystemSink.suppliedConf = conf;

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
    createDirectoriesSecurely();
  }

  /**
   * Stop the mini-DFS cluster.
   */
  @After
  public void stopCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }

    // Restore non-secure conf
    UserGroupInformation.setConfiguration(new Configuration());
    RollingFileSystemSink.suppliedConf = null;
    RollingFileSystemSink.suppliedFilesystem = null;
  }

  /**
   * Stop the mini-KDC.
   */
  @AfterClass
  public static void shutdownKdc() {
    if (kdc != null) {
      kdc.stop();
    }
  }

  /**
   * Do a basic write test against an HDFS cluster with Kerberos enabled. We
   * assume that if a basic write succeeds, more complex operations will also
   * succeed.
   *
   * @throws Exception thrown if things break
   */
  @Test
  public void testWithSecureHDFS() throws Exception {
    final String path =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp/test";
    final MetricsSystem ms =
        initMetricsSystem(path, true, false, true);

    assertMetricsContents(
        sink.doAs(new PrivilegedExceptionAction<String>() {
          @Override
          public String run() throws Exception {
            return doWriteTest(ms, path, 1);
          }
        }));
  }

  /**
   * Do a basic write test against an HDFS cluster with Kerberos enabled but
   * without the principal and keytab properties set.
   *
   * @throws Exception thrown if things break
   */
  @Test
  public void testMissingPropertiesWithSecureHDFS() throws Exception {
    final String path =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp/test";

    initMetricsSystem(path, true, false);

    assertTrue("No exception was generated initializing the sink against a "
        + "secure cluster even though the principal and keytab properties "
        + "were missing", MockSink.errored);
  }

  /**
   * Create the /tmp directory as <i>hdfs</i> and /tmp/test as <i>sink</i> and
   * return the UGI for <i>sink</i>.
   *
   * @throws IOException thrown if login or directory creation fails
   * @throws InterruptedException thrown if interrupted while creating a
   * file system handle
   */
  protected void createDirectoriesSecurely()
      throws IOException, InterruptedException {
    Path tmp = new Path("/tmp");
    Path test = new Path(tmp, "test");

    UserGroupInformation hdfs =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(hdfsPrincipal,
            hdfsKeytab);
    FileSystem fsForSuperUser =
        hdfs.doAs(new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws Exception {
            return cluster.getFileSystem();
          }
        });

    fsForSuperUser.mkdirs(tmp);
    fsForSuperUser.setPermission(tmp, new FsPermission((short)0777));

    sink = UserGroupInformation.loginUserFromKeytabAndReturnUGI(sinkPrincipal,
            sinkKeytab);

    FileSystem fsForSink =
        sink.doAs(new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws Exception {
            return cluster.getFileSystem();
          }
        });

    fsForSink.mkdirs(test);
    RollingFileSystemSink.suppliedFilesystem = fsForSink;
  }

  /**
   * Creates a configuration for starting a secure cluster.
   *
   * @param dataTransferProtection supported QOPs
   * @return configuration for starting a secure cluster
   * @throws Exception if there is any failure
   */
  protected HdfsConfiguration createSecureConfig(
      String dataTransferProtection) throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();

    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(SINK_PRINCIPAL_KEY, sinkPrincipal);
    conf.set(SINK_KEYTAB_FILE_KEY, sinkKeytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, dataTransferProtection);
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUP_MAPPING,
        NullGroupsMapping.class.getName());

    String keystoresDir = methodDir.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(this.getClass());

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    return conf;
  }
}

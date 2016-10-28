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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
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
import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class SaslDataTransferTestCase {

  private static File baseDir;
  private static String keystoresDir;
  private static String sslConfDir;
  private static String hdfsPrincipal;
  private static String userPrincipal;
  private static MiniKdc kdc;
  private static String hdfsKeytab;
  private static String userKeyTab;
  private static String spnegoPrincipal;

  public static String getUserKeyTab() {
    return userKeyTab;
  }

  public static String getUserPrincipal() {
    return userPrincipal;
  }

  public static String getHdfsPrincipal() {
    return hdfsPrincipal;
  }

  public static String getHdfsKeytab() {
    return hdfsKeytab;
  }

  @BeforeClass
  public static void initKdc() throws Exception {
    baseDir = GenericTestUtils
        .getTestDir(SaslDataTransferTestCase.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    String userName = RandomStringUtils.randomAlphabetic(8);
    File userKeytabFile = new File(baseDir, userName + ".keytab");
    userKeyTab = userKeytabFile.getAbsolutePath();
    kdc.createPrincipal(userKeytabFile, userName + "/localhost");
    userPrincipal = userName + "/localhost@" + kdc.getRealm();

    String superUserName = "hdfs";
    File hdfsKeytabFile = new File(baseDir, superUserName + ".keytab");
    hdfsKeytab = hdfsKeytabFile.getAbsolutePath();
    kdc.createPrincipal(hdfsKeytabFile, superUserName + "/localhost", "HTTP/localhost");
    hdfsPrincipal = superUserName + "/localhost@" + kdc.getRealm();
    spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();
  }

  @AfterClass
  public static void shutdownKdc() throws Exception {
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(baseDir);
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  /**
   * Creates configuration for starting a secure cluster.
   *
   * @param dataTransferProtection supported QOPs
   * @return configuration for starting a secure cluster
   * @throws Exception if there is any failure
   */
  protected HdfsConfiguration createSecureConfig(
      String dataTransferProtection) throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, dataTransferProtection);
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);

    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(this.getClass());
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
    return conf;
  }
}

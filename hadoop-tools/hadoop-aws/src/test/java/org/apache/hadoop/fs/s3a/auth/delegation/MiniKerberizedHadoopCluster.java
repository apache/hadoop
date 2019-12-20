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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.KDiag;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig.DEFAULT_MR_HISTORY_PORT;
import static org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytabAndReturnUGI;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;
import static org.junit.Assert.assertTrue;

/**
 * This is intended to support setting up an mini-secure Hadoop + YARN + MR
 * cluster.
 * It does not do this, yet, for the following reason: things don't work.
 * It is designed to be started/stopped at the class level.
 * however, users should be logged in in test cases, so that their local state
 * (credentials etc) are reset in every test.
 */
public class MiniKerberizedHadoopCluster extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(MiniKerberizedHadoopCluster.class);

  public static final String ALICE = "alice";

  public static final String BOB = "bob";

  public static final String HTTP_LOCALHOST = "HTTP/localhost@$LOCALHOST";

  /**
   * The hostname is dynamically determined based on OS, either
   * "localhost" (non-windows) or 127.0.0.1 (windows).
   */
  public static final String LOCALHOST_NAME = Path.WINDOWS
      ? "127.0.0.1"
      : "localhost";

  private MiniKdc kdc;

  private File keytab;

  private File workDir;

  private String krbInstance;

  private String loginUsername;

  private String loginPrincipal;

  private String sslConfDir;

  private String clientSSLConfigFileName;

  private String serverSSLConfigFileName;

  private String alicePrincipal;

  private String bobPrincipal;

  /**
   * Create the cluster.
   * If this class's log is at DEBUG level, this also turns
   * Kerberos diagnostics on in the JVM.
   */
  public MiniKerberizedHadoopCluster() {
    super("MiniKerberizedHadoopCluster");
    // load all the configs to force in the -default.xml files
    new HdfsConfiguration();
    new YarnConfiguration();
    new JobConf();
    if (LOG.isDebugEnabled()) {
      // turn on kerberos logging @ debug.
      System.setProperty(KDiag.SUN_SECURITY_KRB5_DEBUG, "true");
      System.setProperty(KDiag.SUN_SECURITY_SPNEGO_DEBUG, "true");
    }

  }

  public MiniKdc getKdc() {
    return kdc;
  }

  public File getKeytab() {
    return keytab;
  }

  public String getKeytabPath() {
    return keytab.getAbsolutePath();
  }

  public UserGroupInformation createBobUser() throws IOException {
    return loginUserFromKeytabAndReturnUGI(bobPrincipal,
        keytab.getAbsolutePath());
  }

  public UserGroupInformation createAliceUser() throws IOException {
    return loginUserFromKeytabAndReturnUGI(alicePrincipal,
        keytab.getAbsolutePath());
  }

  public File getWorkDir() {
    return workDir;
  }

  public String getKrbInstance() {
    return krbInstance;
  }

  public String getLoginUsername() {
    return loginUsername;
  }

  public String getLoginPrincipal() {
    return loginPrincipal;
  }

  public String withRealm(String user) {
    return user + "@EXAMPLE.COM";
  }

  /**
   * Service init creates the KDC.
   * @param conf configuration
   */
  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    patchConfigAtInit(conf);
    super.serviceInit(conf);
    Properties kdcConf = MiniKdc.createConf();
    workDir = GenericTestUtils.getTestDir("kerberos");
    workDir.mkdirs();
    kdc = new MiniKdc(kdcConf, workDir);

    krbInstance = LOCALHOST_NAME;
  }

  /**
   * Start the KDC, create the keytab and the alice and bob users,
   * and UGI instances of them logged in from the keytab.
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    kdc.start();
    keytab = new File(workDir, "keytab.bin");
    loginUsername = UserGroupInformation.getLoginUser().getShortUserName();
    loginPrincipal = loginUsername + "/" + krbInstance;

    alicePrincipal = ALICE + "/" + krbInstance;
    bobPrincipal = BOB + "/" + krbInstance;
    kdc.createPrincipal(keytab,
        alicePrincipal,
        bobPrincipal,
        "HTTP/" + krbInstance,
        HTTP_LOCALHOST,
        loginPrincipal);
    final File keystoresDir = new File(workDir, "ssl");
    keystoresDir.mkdirs();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
        this.getClass());
    KeyStoreTestUtil.setupSSLConfig(keystoresDir.getAbsolutePath(),
        sslConfDir, getConfig(), false);
    clientSSLConfigFileName = KeyStoreTestUtil.getClientSSLConfigFileName();
    serverSSLConfigFileName = KeyStoreTestUtil.getServerSSLConfigFileName();
  }


  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // this can throw an exception, but it will get caught by the superclass.
    kdc.stop();
  }


  protected void patchConfigAtInit(final Configuration conf) {

    // turn off some noise during debugging
    int timeout = 60 * 60_1000;
    conf.setInt("jvm.pause.info-threshold.ms", timeout);
    conf.setInt("jvm.pause.warn-threshold.ms", timeout);
  }

  /**
   * Set up HDFS to run securely.
   * In secure mode, HDFS goes out of its way to refuse to start if it
   * doesn't consider the configuration safe.
   * This is good in production, and it stops an HDFS cluster coming
   * up where things can't reliably talk to each other.
   * But it does complicate test setup.
   * Look at {@code org.apache.hadoop.hdfs.TestDFSInotifyEventInputStreamKerberized}
   * and {@code org.apache.hadoop.hdfs.qjournal.TestSecureNNWithQJM}
   * for the details on what options to set here.
   * @param conf configuration to patch.
   */
  protected void patchConfigWithHDFSBindings(final Configuration conf) {
    Preconditions.checkState(isInState(STATE.STARTED));
    enableKerberos(conf);

    String path = getKeytabPath();
    String spnegoPrincipal = "*";
    String localhost = LOCALHOST_NAME;
    String instance = getKrbInstance();
    String hdfsPrincipal = getLoginPrincipal();
    patchConfigAtInit(conf);

    conf.setLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY, Long.MAX_VALUE);

    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, path);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, path);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.set(DFS_JOURNALNODE_KEYTAB_FILE_KEY, path);
    conf.set(DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");

    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  /**
   * Patch the YARN settings.
   * Note how the yarn principal has to include the realm.
   * @param conf configuration to patch.
   */
  protected void patchConfigWithYARNBindings(final Configuration conf) {
    Preconditions.checkState(isInState(STATE.STARTED));
    enableKerberos(conf);
    patchConfigAtInit(conf);
    String path = getKeytabPath();
    String localhost = LOCALHOST_NAME;
    String yarnPrincipal = withRealm(getLoginPrincipal());
    conf.set(RM_PRINCIPAL, yarnPrincipal);

    conf.set(RM_KEYTAB, path);
    conf.set(RM_HOSTNAME, localhost);
    conf.set(RM_BIND_HOST, localhost);
    conf.set(RM_ADDRESS,
        localhost + ":" + DEFAULT_RM_PORT);

    conf.set(NM_PRINCIPAL, yarnPrincipal);
    conf.set(NM_KEYTAB, path);
    conf.set(NM_ADDRESS,
        localhost + ":" + DEFAULT_NM_PORT);
    conf.setBoolean(TIMELINE_SERVICE_ENABLED, false);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA, false);

    conf.set(JHAdminConfig.MR_HISTORY_KEYTAB, path);
    conf.set(JHAdminConfig.MR_HISTORY_PRINCIPAL, yarnPrincipal);
    conf.set(JHAdminConfig.MR_HISTORY_ADDRESS,
        localhost + ":" + DEFAULT_MR_HISTORY_PORT);
    conf.setBoolean(JHAdminConfig.MR_HISTORY_CLEANER_ENABLE, false);

    conf.setInt(RM_AM_MAX_ATTEMPTS, 1);
    conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        100);
    conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
        10_000);
  }


  public void resetUGI() {
    UserGroupInformation.reset();
  }

  /**
   * Given a shortname, built a long name with the krb instance and realm info.
   * @param shortname short name of the user
   * @return a long name
   */
  private String userOnHost(final String shortname) {
    return shortname + "/" + krbInstance + "@" + getRealm();
  }

  public String getRealm() {
    return kdc.getRealm();
  }

  /**
   * Log in a user to UGI.currentUser.
   * @param user user to log in from
   * @throws IOException failure
   */
  public void loginUser(final String user) throws IOException {
    UserGroupInformation.loginUserFromKeytab(user, getKeytabPath());
  }

  /**
   * Log in the login principal as the current user.
   * @throws IOException failure
   */
  public void loginPrincipal() throws IOException {
    loginUser(getLoginPrincipal());
  }

  /**
   * General assertion that security is turred on for a cluster.
   */
  public static void assertSecurityEnabled() {
    assertTrue("Security is needed for this test",
        UserGroupInformation.isSecurityEnabled());
  }


  /**
   * Close filesystems for a user, downgrading a null user to a no-op.
   * @param ugi user
   * @throws IOException if a close operation raised one.
   */
  public static void closeUserFileSystems(UserGroupInformation ugi)
      throws IOException {
    if (ugi != null) {
      FileSystem.closeAllForUGI(ugi);
    }
  }

  /**
   * Modify a configuration to use Kerberos as the auth method.
   * @param conf configuration to patch.
   */
  public static void enableKerberos(Configuration conf) {
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
  }
}

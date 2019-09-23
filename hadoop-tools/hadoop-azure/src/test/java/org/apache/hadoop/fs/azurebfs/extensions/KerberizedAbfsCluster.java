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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.KDiag;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytabAndReturnUGI;
import static org.junit.Assert.assertTrue;

/**
 * composite service for adding kerberos login for ABFS
 * tests which require a logged in user.
 * Based on
 * {@code org.apache.hadoop.fs.s3a.auth.delegation.MiniKerberizedHadoopCluster}
 */
public class KerberizedAbfsCluster extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(KerberizedAbfsCluster.class);

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
  public KerberizedAbfsCluster() {
    super("KerberizedAbfsCluster");
    // load all the configs to force in the -default.xml files
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
    String kerberosRule =
        "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT";
      KerberosName.setRules(kerberosRule);
  }


  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // this can throw an exception, but it will get caught by the superclass.
    kdc.stop();
  }


  protected void patchConfigAtInit(final Configuration conf) {

    // turn off some noise during debugging
    int timeout = (int) Duration.ofHours(1).toMillis();
    conf.setInt("jvm.pause.info-threshold.ms", timeout);
    conf.setInt("jvm.pause.warn-threshold.ms", timeout);
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
  public void bindConfToCluster(Configuration conf) {
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
    conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
        "alice,alice");
    // a shortname for the RM principal avoids kerberos mapping problems.
    conf.set(YarnConfiguration.RM_PRINCIPAL, BOB);
  }

  /**
   * Utility method to create a URI, converting URISyntaxException
   * to RuntimeExceptions. This makes it easier to set up URIs
   * in static fields.
   * @param uri URI to create.
   * @return the URI.
   * @throws RuntimeException syntax error.
   */
  public static URI newURI(String uri) {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create the filename for a temporary token file, in the
   * work dir of this cluster.
   * @return a filename which does not exist.
   * @throws IOException failure
   */
  public File createTempTokenFile() throws IOException {
    File tokenfile = File.createTempFile("tokens", ".bin",
        getWorkDir());
    tokenfile.delete();
    return tokenfile;
  }

}

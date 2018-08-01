/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.server.SCMStorage;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to for security enabled Ozone cluster.
 */
@InterfaceAudience.Private
public final class TestSecureOzoneCluster {

  private Logger LOGGER = LoggerFactory
      .getLogger(TestSecureOzoneCluster.class);

  @Rule
  public Timeout timeout = new Timeout(80000);

  private MiniKdc miniKdc;
  private OzoneConfiguration conf;
  private File workDir;
  private static Properties securityProperties;
  private File scmKeytab;
  private File spnegoKeytab;
  private File omKeyTab;
  private String curUser;
  private StorageContainerManager scm;
  private OzoneManager om;

  private static String clusterId;
  private static String scmId;
  private static String omId;

  @Before
  public void init() {
    try {
      conf = new OzoneConfiguration();
      startMiniKdc();
      setSecureConfig(conf);
      createCredentialsInKDC(conf, miniKdc);
    } catch (IOException e) {
      LOGGER.error("Failed to initialize TestSecureOzoneCluster", e);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize TestSecureOzoneCluster", e);
    }
  }

  @After
  public void stop() {
    try {
      stopMiniKdc();
      if (scm != null) {
        scm.stop();
      }
      if (om != null) {
        om.stop();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to stop TestSecureOzoneCluster", e);
    }
  }

  private void createCredentialsInKDC(Configuration conf, MiniKdc miniKdc)
      throws Exception {
    createPrincipal(scmKeytab,
        conf.get(ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY));
     createPrincipal(spnegoKeytab,
         conf.get(ScmConfigKeys
             .HDDS_SCM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY));
        conf.get(OMConfigKeys
            .OZONE_OM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);
    createPrincipal(omKeyTab,
        conf.get(OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY));
  }

  private void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  private void startMiniKdc() throws Exception {
    workDir = GenericTestUtils
        .getTestDir(TestSecureOzoneCluster.class.getSimpleName());
    securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private void stopMiniKdc() throws Exception {
    miniKdc.stop();
  }

  private void setSecureConfig(Configuration conf) throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    String host = KerberosUtil.getLocalHostName();
    String realm = miniKdc.getRealm();
    curUser = UserGroupInformation.getCurrentUser()
        .getUserName();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set(OZONE_ADMINISTRATORS, curUser);

    conf.set(ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/" + host + "@" + realm);
    conf.set(ScmConfigKeys.HDDS_SCM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "HTTP_SCM/" + host + "@" + realm);

    conf.set(OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "om/" + host + "@" + realm);
    conf.set(OMConfigKeys.OZONE_OM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "HTTP_OM/" + host + "@" + realm);

    scmKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    omKeyTab = new File(workDir, "om.keytab");

    conf.set(ScmConfigKeys.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        scmKeytab.getAbsolutePath());
    conf.set(ScmConfigKeys.HDDS_SCM_WEB_AUTHENTICATION_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        omKeyTab.getAbsolutePath());
  }

  @Test
  public void testSecureScmStartupSuccess() throws Exception {

    initSCM();
    scm = StorageContainerManager.createSCM(null, conf);
    //Reads the SCM Info from SCM instance
    ScmInfo scmInfo = scm.getClientProtocolServer().getScmInfo();
    Assert.assertEquals(clusterId, scmInfo.getClusterId());
    Assert.assertEquals(scmId, scmInfo.getScmId());
  }

  private void initSCM()
      throws IOException, AuthenticationException {

    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();

    final String path = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    SCMStorage scmStore = new SCMStorage(conf);
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    // writes the version file properties
    scmStore.initialize();
  }

  @Test
  public void testSecureScmStartupFailure() throws Exception {
    initSCM();
    conf.set(ScmConfigKeys.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, "");
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");

    LambdaTestUtils.intercept(IOException.class,
        "Running in secure mode, but config doesn't have a keytab",
        () -> {
          StorageContainerManager.createSCM(null, conf);
        });

    conf.set(ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/_HOST@EXAMPLE.com");
    conf.set(ScmConfigKeys.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        "/etc/security/keytabs/scm.keytab");

    testCommonKerberosFailures(
        () -> StorageContainerManager.createSCM(null, conf));

  }

  private void testCommonKerberosFailures(Callable callable) throws Exception {
    LambdaTestUtils.intercept(KerberosAuthException.class, "failure "
        + "to login: for principal:", callable);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "OAuth2");

    LambdaTestUtils.intercept(IllegalArgumentException.class, "Invalid"
            + " attribute value for hadoop.security.authentication of OAuth2",
        callable);

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "KERBEROS_SSL");
    LambdaTestUtils.intercept(AuthenticationException.class,
        "KERBEROS_SSL authentication method not",
        callable);
  }

  /**
   * Tests the secure KSM Initialization Failure.
   *
   * @throws IOException
   */
  @Test
  public void testSecureKsmInitializationFailure() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = StorageContainerManager.createSCM(null, conf);

    final String path = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    OMStorage ksmStore = new OMStorage(conf);
    ksmStore.setClusterId("testClusterId");
    ksmStore.setScmId("testScmId");
    // writes the version file properties
    ksmStore.initialize();
    conf.set(OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "non-existent-user@EXAMPLE.com");
    testCommonKerberosFailures(() -> OzoneManager.createOm(null, conf));
  }

  /**
   * Tests the secure KSM Initialization success.
   *
   * @throws IOException
   */
  @Test
  public void testSecureKsmInitializationSuccess() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = StorageContainerManager.createSCM(null, conf);
    LogCapturer logs = LogCapturer.captureLogs(OzoneManager.LOG);
    GenericTestUtils
        .setLogLevel(LoggerFactory.getLogger(OzoneManager.class.getName()),
            org.slf4j.event.Level.INFO);

    final String path = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    Path metaDirPath = Paths.get(path, "om-meta");

    OMStorage omStore = new OMStorage(conf);
    omStore.setClusterId("testClusterId");
    omStore.setScmId("testScmId");
    // writes the version file properties
    omStore.initialize();
    try {
      om = OzoneManager.createOm(null, conf);
    } catch (Exception ex) {
      // Expects timeout failure from scmClient in KSM but KSM user login via
      // kerberos should succeed
      Assert.assertTrue(logs.getOutput().contains("KSM login successful"));
    }
  }

}

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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.slf4j.event.Level.INFO;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.server.SCMStorage;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
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

  private static final String TEST_USER = "testUgiUser";
  private static final int CLIENT_TIMEOUT = 2 * 1000;
  private Logger logger = LoggerFactory
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
  private OzoneManagerProtocolClientSideTranslatorPB omClient;
  private KeyPair keyPair;
  private Path metaDirPath;

  @Before
  public void init() {
    try {
      conf = new OzoneConfiguration();
      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
      DefaultMetricsSystem.setMiniClusterMode(true);
      final String path = GenericTestUtils
          .getTempPath(UUID.randomUUID().toString());
      metaDirPath = Paths.get(path, "om-meta");
      conf.set(OZONE_METADATA_DIRS, metaDirPath.toString());
      startMiniKdc();
      setSecureConfig(conf);
      createCredentialsInKDC(conf, miniKdc);
      generateKeyPair(conf);
    } catch (IOException e) {
      logger.error("Failed to initialize TestSecureOzoneCluster", e);
    } catch (Exception e) {
      logger.error("Failed to initialize TestSecureOzoneCluster", e);
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
      if (omClient != null) {
        omClient.close();
      }
      FileUtils.deleteQuietly(metaDirPath.toFile());
    } catch (Exception e) {
      logger.error("Failed to stop TestSecureOzoneCluster", e);
    }
  }

  private void createCredentialsInKDC(Configuration conf, MiniKdc miniKdc)
      throws Exception {
    createPrincipal(scmKeytab,
        conf.get(ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY));
    createPrincipal(spnegoKeytab,
        conf.get(ScmConfigKeys
            .HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY));
    conf.get(OMConfigKeys
        .OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY);
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

  private void stopMiniKdc() {
    miniKdc.stop();
  }

  private void setSecureConfig(Configuration conf) throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setBoolean(OZONE_ENABLED, true);
    String host = InetAddress.getLocalHost().getCanonicalHostName();
    String realm = miniKdc.getRealm();
    curUser = UserGroupInformation.getCurrentUser()
        .getUserName();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set(OZONE_ADMINISTRATORS, curUser);

    conf.set(ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/" + host + "@" + realm);
    conf.set(ScmConfigKeys.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY,
        "HTTP_SCM/" + host + "@" + realm);

    conf.set(OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "om/" + host + "@" + realm);
    conf.set(OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY,
        "HTTP_OM/" + host + "@" + realm);

    scmKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    omKeyTab = new File(workDir, "om.keytab");

    conf.set(ScmConfigKeys.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        scmKeytab.getAbsolutePath());
    conf.set(
        ScmConfigKeys.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        omKeyTab.getAbsolutePath());
    conf.set(OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());
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
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
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
   * Tests the secure om Initialization Failure.
   *
   * @throws IOException
   */
  @Test
  public void testSecureOMInitializationFailure() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = StorageContainerManager.createSCM(null, conf);

    setupOm(conf);
    conf.set(OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "non-existent-user@EXAMPLE.com");
    testCommonKerberosFailures(() -> OzoneManager.createOm(null, conf));
  }

  /**
   * Tests the secure om Initialization success.
   *
   * @throws IOException
   */
  @Test
  public void testSecureOmInitializationSuccess() throws Exception {
    initSCM();
    // Create a secure SCM instance as om client will connect to it
    scm = StorageContainerManager.createSCM(null, conf);
    LogCapturer logs = LogCapturer.captureLogs(OzoneManager.LOG);
    GenericTestUtils.setLogLevel(OzoneManager.LOG, INFO);

    setupOm(conf);
    try {
      om.start();
    } catch (Exception ex) {
      // Expects timeout failure from scmClient in om but om user login via
      // kerberos should succeed.
      Assert.assertTrue(logs.getOutput().contains("Ozone Manager login"
          + " successful"));
    }
  }

  /**
   * Performs following tests for delegation token.
   * 1. Get valid delegation token
   * 2. Test successful token renewal.
   * 3. Client can authenticate using token.
   * 4. Delegation token renewal without Kerberos auth fails.
   * 5. Test success of token cancellation.
   * 5. Test failure of token cancellation.
   *
   * @throws Exception
   */
  @Test
  public void testDelegationToken() throws Exception {

    // Capture logs for assertions
    LogCapturer logs = LogCapturer.captureLogs(Server.AUDITLOG);
    LogCapturer omLogs = LogCapturer.captureLogs(OzoneManager.getLogger());
    GenericTestUtils
        .setLogLevel(LoggerFactory.getLogger(Server.class.getName()), INFO);

    // Setup secure OM for start
    setupOm(conf);
    long omVersion =
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    try {
      // Start OM
      om.start();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getUserName();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), ugi, conf,
              NetUtils.getDefaultSocketFactory(conf),
              CLIENT_TIMEOUT), RandomStringUtils.randomAscii(5));

      // Assert if auth was successful via Kerberos
      Assert.assertFalse(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:KERBEROS)"));

      // Case 1: Test successful delegation token.
      Token<OzoneTokenIdentifier> token = omClient
          .getDelegationToken(new Text("om"));

      // Case 2: Test successful token renewal.
      long renewalTime = omClient.renewDelegationToken(token);
      Assert.assertTrue(renewalTime > 0);

      // Check if token is of right kind and renewer is running om instance
      Assert.assertEquals(token.getKind().toString(), "OzoneToken");
      Assert.assertEquals(token.getService().toString(),
          OmUtils.getOmRpcAddress(conf));
      omClient.close();

      // Create a remote ugi and set its authentication method to Token
      UserGroupInformation testUser = UserGroupInformation
          .createRemoteUser(TEST_USER);
      testUser.addToken(token);
      testUser.setAuthenticationMethod(AuthMethod.TOKEN);
      UserGroupInformation.setLoginUser(testUser);

      // Get Om client, this time authentication should happen via Token
      testUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          omClient = new OzoneManagerProtocolClientSideTranslatorPB(
              RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
                  OmUtils.getOmAddress(conf), testUser, conf,
                  NetUtils.getDefaultSocketFactory(conf), CLIENT_TIMEOUT),
              RandomStringUtils.randomAscii(5));
          return null;
        }
      });

      // Case 3: Test Client can authenticate using token.
      Assert.assertFalse(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:TOKEN)"));
      LambdaTestUtils.intercept(IOException.class, "Delete Volume failed,"
          + " error:VOLUME_NOT_FOUND", () -> omClient.deleteVolume("vol1"));
      Assert.assertTrue(logs.getOutput().contains("Auth successful for "
          + username + " (auth:TOKEN)"));

      // Case 4: Test failure of token renewal.
      // Call to renewDelegationToken will fail but it will confirm that
      // initial connection via DT succeeded
      omLogs.clearOutput();

      LambdaTestUtils.intercept(OMException.class, "Renew delegation token " +
              "failed",
          () -> {
            try {
              omClient.renewDelegationToken(token);
            } catch (OMException ex) {
              Assert.assertTrue(ex.getResult().equals(INVALID_AUTH_METHOD));
              throw ex;
            }
          });
      Assert.assertTrue(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:TOKEN)"));
      omLogs.clearOutput();
      //testUser.setAuthenticationMethod(AuthMethod.KERBEROS);
      UserGroupInformation.setLoginUser(ugi);
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), ugi, conf,
              NetUtils.getDefaultSocketFactory(conf),
              Client.getRpcTimeout(conf)), RandomStringUtils.randomAscii(5));

      // Case 5: Test success of token cancellation.
      omClient.cancelDelegationToken(token);
      omClient.close();

      // Wait for client to timeout
      Thread.sleep(CLIENT_TIMEOUT);

      Assert.assertFalse(logs.getOutput().contains("Auth failed for"));

      // Case 6: Test failure of token cancellation.
      // Get Om client, this time authentication using Token will fail as
      // token is not in cache anymore.
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), testUser, conf,
              NetUtils.getDefaultSocketFactory(conf),
              Client.getRpcTimeout(conf)), RandomStringUtils.randomAscii(5));
      LambdaTestUtils.intercept(OMException.class, "Cancel delegation " +
              "token failed",
          () -> {
            try {
              omClient.cancelDelegationToken(token);
            } catch (OMException ex) {
              Assert.assertTrue(ex.getResult().equals(TOKEN_ERROR_OTHER));
              throw ex;
            }
          });

      Assert.assertTrue(logs.getOutput().contains("Auth failed for"));
    } finally {
      om.stop();
      om.join();
    }
  }

  private void generateKeyPair(OzoneConfiguration config) throws Exception {
    HDDSKeyGenerator keyGenerator = new HDDSKeyGenerator(conf);
    keyPair = keyGenerator.generateKey();
    KeyCodec pemWriter = new KeyCodec(config);
    pemWriter.writeKey(keyPair, true);
  }

  /**
   * Tests delegation token renewal.
   *
   * @throws Exception
   */
  @Test
  public void testDelegationTokenRenewal() throws Exception {
    GenericTestUtils
        .setLogLevel(LoggerFactory.getLogger(Server.class.getName()), INFO);
    LogCapturer omLogs = LogCapturer.captureLogs(OzoneManager.getLogger());

    // Setup secure OM for start.
    OzoneConfiguration newConf = new OzoneConfiguration(conf);
    newConf.setLong(OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY, 500);
    setupOm(newConf);
    long omVersion =
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    OzoneManager.setTestSecureOmFlag(true);
    // Start OM

    try {
      om.start();

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(RPC.getProxy(
          OzoneManagerProtocolPB.class, omVersion, OmUtils.getOmAddress(conf),
          ugi, conf, NetUtils.getDefaultSocketFactory(conf),
          CLIENT_TIMEOUT), RandomStringUtils.randomAscii(5));

      // Since client is already connected get a delegation token
      Token<OzoneTokenIdentifier> token = omClient.getDelegationToken(
          new Text("om"));

      // Check if token is of right kind and renewer is running om instance
      Assert.assertEquals(token.getKind().toString(), "OzoneToken");
      Assert.assertEquals(token.getService().toString(), OmUtils
          .getOmRpcAddress(conf));

      // Renew delegation token
      long expiryTime = omClient.renewDelegationToken(token);
      Assert.assertTrue(expiryTime > 0);
      omLogs.clearOutput();

      // Test failure of delegation renewal
      // 1. When token maxExpiryTime exceeds
      Thread.sleep(500);
      LambdaTestUtils.intercept(OMException.class,
          "Renew delegation token failed",
          () -> {
            try {
              omClient.renewDelegationToken(token);
            } catch (OMException ex) {
              Assert.assertTrue(ex.getResult().equals(TOKEN_EXPIRED));
              throw ex;
            }
          });

      omLogs.clearOutput();

      // 2. When renewer doesn't match (implicitly covers when renewer is
      // null or empty )
      Token token2 = omClient.getDelegationToken(new Text("randomService"));
      LambdaTestUtils.intercept(OMException.class,
          "Renew delegation token failed",
          () -> omClient.renewDelegationToken(token2));
      Assert.assertTrue(omLogs.getOutput().contains(" with non-matching " +
          "renewer randomService"));
      omLogs.clearOutput();

      // 3. Test tampered token
      OzoneTokenIdentifier tokenId = OzoneTokenIdentifier.readProtoBuf(
          token.getIdentifier());
      tokenId.setRenewer(new Text("om"));
      tokenId.setMaxDate(System.currentTimeMillis() * 2);
      Token<OzoneTokenIdentifier> tamperedToken = new Token<>(
          tokenId.getBytes(), token2.getPassword(), token2.getKind(),
          token2.getService());
      LambdaTestUtils.intercept(OMException.class,
          "Renew delegation token failed",
          () -> omClient.renewDelegationToken(tamperedToken));
      Assert.assertTrue(omLogs.getOutput().contains("can't be found in " +
          "cache"));
      omLogs.clearOutput();

    } finally {
      om.stop();
      om.join();
    }
  }

  private void setupOm(OzoneConfiguration config) throws Exception {
    OMStorage omStore = new OMStorage(config);
    omStore.setClusterId("testClusterId");
    omStore.setScmId("testScmId");
    // writes the version file properties
    omStore.initialize();
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(null, config);
    CertificateClient certClient = new CertificateClientTestImpl(config);
    om.setCertClient(certClient);
  }

}

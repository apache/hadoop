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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
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
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_AUTH_METHOD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.slf4j.event.Level.INFO;

/**
 * Test class to for security enabled Ozone cluster.
 */
@InterfaceAudience.Private
public final class TestSecureOzoneCluster {

  private static final String TEST_USER = "testUgiUser@EXAMPLE.COM";
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
  private File testUserKeytab;
  private String curUser;
  private String testUserPrincipal;
  private UserGroupInformation testKerberosUgi;
  private StorageContainerManager scm;
  private OzoneManager om;
  private String host;

  private static String clusterId;
  private static String scmId;
  private static String omId;
  private OzoneManagerProtocolClientSideTranslatorPB omClient;
  private KeyPair keyPair;
  private Path metaDirPath;
  @Rule
  public TemporaryFolder folder= new TemporaryFolder();
  private String omCertSerialId = "9879877970576";

  @Before
  public void init() {
    try {
      conf = new OzoneConfiguration();
      conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
      DefaultMetricsSystem.setMiniClusterMode(true);
      final String path = folder.newFolder().toString();
      metaDirPath = Paths.get(path, "om-meta");
      conf.set(OZONE_METADATA_DIRS, metaDirPath.toString());
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          KERBEROS.toString());

      startMiniKdc();
      setSecureConfig(conf);
      createCredentialsInKDC(conf, miniKdc);
      generateKeyPair(conf);
//      OzoneManager.setTestSecureOmFlag(true);
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
    } catch (Exception e) {
      logger.error("Failed to stop TestSecureOzoneCluster", e);
    }
  }

  private void createCredentialsInKDC(Configuration configuration,
                                      MiniKdc kdc) throws Exception {
    createPrincipal(scmKeytab,
        configuration.get(ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY));
    createPrincipal(spnegoKeytab,
        configuration.get(ScmConfigKeys
            .HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY));
    createPrincipal(testUserKeytab, testUserPrincipal);
    createPrincipal(omKeyTab,
        configuration.get(OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY));
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

  private void setSecureConfig(Configuration configuration) throws IOException {
    configuration.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    configuration.setBoolean(OZONE_ENABLED, true);
    host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();
    String realm = miniKdc.getRealm();
    curUser = UserGroupInformation.getCurrentUser()
        .getUserName();
    configuration.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    configuration.set(OZONE_ADMINISTRATORS, curUser);

    configuration.set(ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/" + host + "@" + realm);
    configuration.set(ScmConfigKeys.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY,
        "HTTP_SCM/" + host + "@" + realm);

    configuration.set(OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "om/" + host + "@" + realm);
    configuration.set(OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY,
        "HTTP_OM/" + host + "@" + realm);

    scmKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    omKeyTab = new File(workDir, "om.keytab");
    testUserKeytab = new File(workDir, "testuser.keytab");
    testUserPrincipal = "test@" + realm;

    configuration.set(ScmConfigKeys.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        scmKeytab.getAbsolutePath());
    configuration.set(
        ScmConfigKeys.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    configuration.set(OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
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

  @Test
  public void testSCMSecurityProtocol() throws Exception {

    initSCM();
    scm = StorageContainerManager.createSCM(null, conf);
    //Reads the SCM Info from SCM instance
    try {
      scm.start();

      // Case 1: User with Kerberos credentials should succeed.
      UserGroupInformation ugi =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(
              testUserPrincipal, testUserKeytab.getCanonicalPath());
      ugi.setAuthenticationMethod(KERBEROS);
      SCMSecurityProtocol scmSecurityProtocolClient =
          HddsClientUtils.getScmSecurityClient(conf, ugi);
      assertNotNull(scmSecurityProtocolClient);
      String caCert = scmSecurityProtocolClient.getCACertificate();
      LambdaTestUtils.intercept(RemoteException.class, "Certificate not found",
          () -> scmSecurityProtocolClient.getCertificate("1"));
      assertNotNull(caCert);

      // Case 2: User without Kerberos credentials should fail.
      ugi = UserGroupInformation.createRemoteUser("test");
      ugi.setAuthenticationMethod(AuthMethod.TOKEN);
      SCMSecurityProtocol finalScmSecurityProtocolClient =
          HddsClientUtils.getScmSecurityClient(conf, ugi);

      LambdaTestUtils.intercept(IOException.class, "Client cannot" +
              " authenticate via:[KERBEROS]",
          () -> finalScmSecurityProtocolClient.getCACertificate());
      LambdaTestUtils.intercept(IOException.class, "Client cannot" +
              " authenticate via:[KERBEROS]",
          () -> finalScmSecurityProtocolClient.getCertificate("1"));
    } finally {
      if (scm != null) {
        scm.stop();
      }
    }
  }

  private void initSCM()
      throws IOException, AuthenticationException {

    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();

    final String path = folder.newFolder().toString();
    Path scmPath = Paths.get(path, "scm-meta");
    File temp = scmPath.toFile();
    if(!temp.exists()) {
      temp.mkdirs();
    }
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
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
      assertTrue(logs.getOutput().contains("Ozone Manager login"
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
      om.setCertClient(new CertificateClientTestImpl(conf));
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
      assertFalse(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:KERBEROS)"));

      // Case 1: Test successful delegation token.
      Token<OzoneTokenIdentifier> token = omClient
          .getDelegationToken(new Text("om"));

      // Case 2: Test successful token renewal.
      long renewalTime = omClient.renewDelegationToken(token);
      assertTrue(renewalTime > 0);

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
      assertFalse(logs.getOutput().contains(
          "Auth successful for " + username + " (auth:TOKEN)"));
      OzoneTestUtils.expectOmException(VOLUME_NOT_FOUND,
          () -> omClient.deleteVolume("vol1"));
      assertTrue(logs.getOutput().contains("Auth successful for "
          + username + " (auth:TOKEN)"));

      // Case 4: Test failure of token renewal.
      // Call to renewDelegationToken will fail but it will confirm that
      // initial connection via DT succeeded
      omLogs.clearOutput();

      LambdaTestUtils.intercept(OMException.class, "INVALID_AUTH_METHOD",
          () -> {
            try {
              omClient.renewDelegationToken(token);
            } catch (OMException ex) {
              assertTrue(ex.getResult().equals(INVALID_AUTH_METHOD));
              throw ex;
            }
          });
      assertTrue(logs.getOutput().contains(
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

      assertFalse(logs.getOutput().contains("Auth failed for"));

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
              assertTrue(ex.getResult().equals(TOKEN_ERROR_OTHER));
              throw ex;
            }
          });

      assertTrue(logs.getOutput().contains("Auth failed for"));
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
      om.setCertClient(new CertificateClientTestImpl(conf));
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
      assertTrue(expiryTime > 0);
      omLogs.clearOutput();

      // Test failure of delegation renewal
      // 1. When token maxExpiryTime exceeds
      Thread.sleep(500);
      LambdaTestUtils.intercept(OMException.class,
          "TOKEN_EXPIRED",
          () -> {
            try {
              omClient.renewDelegationToken(token);
            } catch (OMException ex) {
              assertTrue(ex.getResult().equals(TOKEN_EXPIRED));
              throw ex;
            }
          });

      omLogs.clearOutput();

      // 2. When renewer doesn't match (implicitly covers when renewer is
      // null or empty )
      Token token2 = omClient.getDelegationToken(new Text("randomService"));
      LambdaTestUtils.intercept(OMException.class,
          "Delegation token renewal failed",
          () -> omClient.renewDelegationToken(token2));
      assertTrue(omLogs.getOutput().contains(" with non-matching " +
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
          "Delegation token renewal failed",
          () -> omClient.renewDelegationToken(tamperedToken));
      assertTrue(omLogs.getOutput().contains("can't be found in " +
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
    omStore.setOmCertSerialId(omCertSerialId);
    // writes the version file properties
    omStore.initialize();
    OzoneManager.setTestSecureOmFlag(true);
    om = OzoneManager.createOm(null, config);
  }

  @Test
  public void testGetS3Secret() throws Exception {

    // Setup secure OM for start
    setupOm(conf);
    long omVersion =
        RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    try {
      // Start OM
      om.setCertClient(new CertificateClientTestImpl(conf));
      om.start();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String username = ugi.getUserName();

      // Get first OM client which will authenticate via Kerberos
      omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              OmUtils.getOmAddress(conf), ugi, conf,
              NetUtils.getDefaultSocketFactory(conf),
              CLIENT_TIMEOUT), RandomStringUtils.randomAscii(5));

      //Creates a secret since it does not exist
      S3SecretValue firstAttempt = omClient
          .getS3Secret("HADOOP/JOHNDOE");

      //Fetches the secret from db since it was created in previous step
      S3SecretValue secondAttempt = omClient
          .getS3Secret("HADOOP/JOHNDOE");

      //secret fetched on both attempts must be same
      assertTrue(firstAttempt.getAwsSecret()
          .equals(secondAttempt.getAwsSecret()));

      //access key fetched on both attempts must be same
      assertTrue(firstAttempt.getAwsAccessKey()
          .equals(secondAttempt.getAwsAccessKey()));

    } finally {
      if(om != null){
        om.stop();
      }
    }
  }

  /**
   * Tests functionality to init secure OM when it is already initialized.
   */
  @Test
  public void testSecureOmReInit() throws Exception {
    LogCapturer omLogs =
        LogCapturer.captureLogs(OzoneManager.getLogger());
    omLogs.clearOutput();
    initSCM();
    try {
      scm = StorageContainerManager.createSCM(null, conf);
      scm.start();
      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, false);
      OMStorage omStore = new OMStorage(conf);
      initializeOmStorage(omStore);
      OzoneManager.setTestSecureOmFlag(true);
      om = OzoneManager.createOm(null, conf);

      assertNull(om.getCertificateClient());
      assertFalse(omLogs.getOutput().contains("Init response: GETCERT"));
      assertFalse(omLogs.getOutput().contains("Successfully stored " +
          "SCM signed certificate"));

      conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
      OzoneManager.omInit(conf);
      om.stop();
      om = OzoneManager.createOm(null, conf);

      Assert.assertNotNull(om.getCertificateClient());
      Assert.assertNotNull(om.getCertificateClient().getPublicKey());
      Assert.assertNotNull(om.getCertificateClient().getPrivateKey());
      Assert.assertNotNull(om.getCertificateClient().getCertificate());
      assertTrue(omLogs.getOutput().contains("Init response: GETCERT"));
      assertTrue(omLogs.getOutput().contains("Successfully stored " +
          "SCM signed certificate"));
      X509Certificate certificate = om.getCertificateClient().getCertificate();
      validateCertificate(certificate);

    } finally {
      if (scm != null) {
        scm.stop();
      }
    }

  }

  /**
   * Test functionality to get SCM signed certificate for OM.
   */
  @Test
  public void testSecureOmInitSuccess() throws Exception {
    LogCapturer omLogs =
        LogCapturer.captureLogs(OzoneManager.getLogger());
    omLogs.clearOutput();
    initSCM();
    try {
      scm = StorageContainerManager.createSCM(null, conf);
      scm.start();

      OMStorage omStore = new OMStorage(conf);
      initializeOmStorage(omStore);
      OzoneManager.setTestSecureOmFlag(true);
      om = OzoneManager.createOm(null, conf);

      Assert.assertNotNull(om.getCertificateClient());
      Assert.assertNotNull(om.getCertificateClient().getPublicKey());
      Assert.assertNotNull(om.getCertificateClient().getPrivateKey());
      Assert.assertNotNull(om.getCertificateClient().getCertificate());
      assertTrue(omLogs.getOutput().contains("Init response: GETCERT"));
      assertTrue(omLogs.getOutput().contains("Successfully stored " +
          "SCM signed certificate"));
      X509Certificate certificate = om.getCertificateClient().getCertificate();
      validateCertificate(certificate);
    } finally {
      if (scm != null) {
        scm.stop();
      }
      if (om != null) {
        om.stop();
      }

    }

  }

  public void validateCertificate(X509Certificate cert) throws Exception {

    // Assert that we indeed have a self signed certificate.
    X500Name x500Issuer = new JcaX509CertificateHolder(cert).getIssuer();
    RDN cn = x500Issuer.getRDNs(BCStyle.CN)[0];
    String hostName = InetAddress.getLocalHost().getHostName();
    String scmUser = "scm@" + hostName;
    Assert.assertEquals(scmUser, cn.getFirst().getValue().toString());

    // Subject name should be om login user in real world but in this test
    // UGI has scm user context.
    Assert.assertEquals(scmUser, cn.getFirst().getValue().toString());

    LocalDate today = LocalDateTime.now().toLocalDate();
    Date invalidDate;

    // Make sure the end date is honored.
    invalidDate = java.sql.Date.valueOf(today.plus(1, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().after(invalidDate));

    invalidDate = java.sql.Date.valueOf(today.plus(400, ChronoUnit.DAYS));
    assertTrue(cert.getNotAfter().before(invalidDate));

    assertTrue(cert.getSubjectDN().toString().contains(scmId));
    assertTrue(cert.getSubjectDN().toString().contains(clusterId));

    assertTrue(cert.getIssuerDN().toString().contains(scmUser));
    assertTrue(cert.getIssuerDN().toString().contains(scmId));
    assertTrue(cert.getIssuerDN().toString().contains(clusterId));

    // Verify that certificate matches the public key.
    String encodedKey1 = cert.getPublicKey().toString();
    String encodedKey2 = om.getCertificateClient().getPublicKey().toString();
    Assert.assertEquals(encodedKey1, encodedKey2);
  }

  private void initializeOmStorage(OMStorage omStorage) throws IOException {
    if (omStorage.getState() == Storage.StorageState.INITIALIZED) {
      return;
    }
    omStorage.setClusterId(clusterId);
    omStorage.setScmId(scmId);
    omStorage.setOmId(omId);
    // Initialize ozone certificate client if security is enabled.
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      OzoneManager.initializeSecurity(conf, omStorage);
    }
    omStorage.initialize();
  }
}

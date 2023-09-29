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

package org.apache.hadoop.yarn.server.timeline.security;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_HTTP_AUTH_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test cases for authentication via TimelineAuthenticationFilter while
 * publishing entities for ATSv1.
 */
public class TestTimelineAuthenticationFilterForV1 {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTimelineAuthenticationFilterForV1.class);

  private static final String FOO_USER = "foo";
  private static final String BAR_USER = "bar";
  private static final String HTTP_USER = "HTTP";
  private static final String PRINCIPAL = HTTP_USER + "/localhost";

  private static final File TEST_ROOT_DIR = new File(
      System.getProperty("test.build.dir", "target/test-dir"),
      TestTimelineAuthenticationFilterForV1.class.getName() + "-root");
  private static final File httpSpnegoKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());
  private static final String httpSpnegoPrincipal =
      KerberosTestUtils.getServerPrincipal();
  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/"
          + TestTimelineAuthenticationFilterForV1.class.getSimpleName();

  public static Collection<Object[]> withSsl() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  private static MiniKdc testMiniKDC;
  private static String keystoresDir;
  private static String sslConfDir;
  private static ApplicationHistoryServer testTimelineServer;
  private static Configuration conf;
  private static boolean withSsl;

  public void initTestTimelineAuthenticationFilterForV1(boolean isSslEnabled) {
    TestTimelineAuthenticationFilterForV1.withSsl = isSslEnabled;
  }

  @BeforeAll
  public static void setup() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), TEST_ROOT_DIR);
      testMiniKDC.start();
      testMiniKDC.createPrincipal(
          httpSpnegoKeytabFile, PRINCIPAL);
    } catch (Exception e) {
      LOG.error("Failed to setup MiniKDC", e);
      fail("Couldn't setup MiniKDC");
    }

    try {
      testTimelineServer = new ApplicationHistoryServer();
      conf = new Configuration(false);
      conf.setStrings(TIMELINE_HTTP_AUTH_PREFIX + "type", "kerberos");
      conf.set(TIMELINE_HTTP_AUTH_PREFIX +
          KerberosAuthenticationHandler.PRINCIPAL, httpSpnegoPrincipal);
      conf.set(TIMELINE_HTTP_AUTH_PREFIX +
          KerberosAuthenticationHandler.KEYTAB,
          httpSpnegoKeytabFile.getAbsolutePath());
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      conf.set(YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL,
          httpSpnegoPrincipal);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
          httpSpnegoKeytabFile.getAbsolutePath());
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STORE,
          MemoryTimelineStore.class, TimelineStore.class);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
          "localhost:10200");
      conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          "localhost:8188");
      conf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          "localhost:8190");
      conf.set("hadoop.proxyuser.HTTP.hosts", "*");
      conf.set("hadoop.proxyuser.HTTP.users", FOO_USER);
      conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES, 1);

      if (withSsl) {
        conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY,
            HttpConfig.Policy.HTTPS_ONLY.name());
        File base = new File(BASEDIR);
        FileUtil.fullyDelete(base);
        base.mkdirs();
        keystoresDir = new File(BASEDIR).getAbsolutePath();
        sslConfDir = KeyStoreTestUtil.getClasspathDir(
            TestTimelineAuthenticationFilterForV1.class);
        KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
      }

      UserGroupInformation.setConfiguration(conf);
      testTimelineServer.init(conf);
      testTimelineServer.start();
    } catch (Exception e) {
      LOG.error("Failed to setup TimelineServer", e);
      fail("Couldn't setup TimelineServer");
    }
  }

  private TimelineClient createTimelineClientForUGI() {
    TimelineClient client = TimelineClient.createTimelineClient();
    client.init(conf);
    client.start();
    return client;
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }

    if (testTimelineServer != null) {
      testTimelineServer.stop();
    }

    if (withSsl) {
      KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
      File base = new File(BASEDIR);
      FileUtil.fullyDelete(base);
    }
  }

  @MethodSource("withSsl")
  @ParameterizedTest
  void testPutTimelineEntities(boolean isSslEnabled) throws Exception {
    initTestTimelineAuthenticationFilterForV1(isSslEnabled);
    KerberosTestUtils.doAs(PRINCIPAL, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        TimelineClient client = createTimelineClientForUGI();
        TimelineEntity entityToStore = new TimelineEntity();
        entityToStore.setEntityType(
            TestTimelineAuthenticationFilterForV1.class.getName());
        entityToStore.setEntityId("entity1");
        entityToStore.setStartTime(0L);
        TimelinePutResponse putResponse = client.putEntities(entityToStore);
        if (putResponse.getErrors().size() > 0) {
          LOG.error("putResponse errors: {}", putResponse.getErrors());
        }
        assertTrue(putResponse.getErrors().isEmpty(),
            "There were some errors in the putResponse");
        TimelineEntity entityToRead =
            testTimelineServer.getTimelineStore().getEntity("entity1",
                TestTimelineAuthenticationFilterForV1.class.getName(), null);
        assertNotNull(entityToRead,
            "Timeline entity should not be null");
        return null;
      }
    });
  }

  @MethodSource("withSsl")
  @ParameterizedTest
  void testPutDomains(boolean isSslEnabled) throws Exception {
    initTestTimelineAuthenticationFilterForV1(isSslEnabled);
    KerberosTestUtils.doAs(PRINCIPAL, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        TimelineClient client = createTimelineClientForUGI();
        TimelineDomain domainToStore = new TimelineDomain();
        domainToStore.setId(
            TestTimelineAuthenticationFilterForV1.class.getName());
        domainToStore.setReaders("*");
        domainToStore.setWriters("*");
        client.putDomain(domainToStore);
        TimelineDomain domainToRead =
            testTimelineServer.getTimelineStore().getDomain(
                TestTimelineAuthenticationFilterForV1.class.getName());
        assertNotNull(domainToRead,
            "Timeline domain should not be null");
        return null;
      }
    });
  }

  @MethodSource("withSsl")
  @ParameterizedTest
  void testDelegationTokenOperations(boolean isSslEnabled) throws Exception {
    initTestTimelineAuthenticationFilterForV1(isSslEnabled);
    TimelineClient httpUserClient =
        KerberosTestUtils.doAs(PRINCIPAL,
            new Callable<TimelineClient>() {
              @Override
              public TimelineClient call() throws Exception {
                return createTimelineClientForUGI();
              }
            });
    UserGroupInformation httpUser =
        KerberosTestUtils.doAs(PRINCIPAL,
            new Callable<UserGroupInformation>() {
              @Override
              public UserGroupInformation call() throws Exception {
                return UserGroupInformation.getCurrentUser();
              }
            });

    // Let HTTP user to get the delegation for itself
    Token<TimelineDelegationTokenIdentifier> token =
        httpUserClient.getDelegationToken(httpUser.getShortUserName());
    assertNotNull(token, "Delegation token should not be null");
    TimelineDelegationTokenIdentifier tDT = token.decodeIdentifier();
    assertNotNull(tDT,
        "Delegation token identifier should not be null");
    assertEquals(new Text(HTTP_USER), tDT.getOwner(),
        "Owner of delegation token identifier does not match");

    // Renew token
    assertFalse(token.getService().toString().isEmpty(),
        "Service field of token should not be empty");
    // Renew the token from the token service address
    long renewTime1 = httpUserClient.renewDelegationToken(token);
    Thread.sleep(100);
    token.setService(new Text());
    assertTrue(token.getService().toString().isEmpty(),
        "Service field of token should be empty");
    // If the token service address is not available, it still can be renewed
    // from the configured address
    long renewTime2 = httpUserClient.renewDelegationToken(token);
    assertTrue(renewTime1 < renewTime2,
        "renewTime2 should be later than renewTime1");

    // Cancel token
    assertTrue(token.getService().toString().isEmpty(),
        "Service field of token should be empty");
    // If the token service address is not available, it still can be canceled
    // from the configured address
    httpUserClient.cancelDelegationToken(token);
    // Renew should not be successful because the token is canceled
    try {
      httpUserClient.renewDelegationToken(token);
      fail("Renew of delegation token should not be successful");
    } catch (Exception e) {
      LOG.info("Exception while renewing delegation token", e);
      assertTrue(e.getMessage().contains(
          "Renewal request for unknown token"));
    }

    // Let HTTP user to get the delegation token for FOO user
    UserGroupInformation fooUgi = UserGroupInformation.createProxyUser(
        FOO_USER, httpUser);
    TimelineClient fooUserClient = fooUgi.doAs(
        new PrivilegedExceptionAction<TimelineClient>() {
          @Override
          public TimelineClient run() {
            return createTimelineClientForUGI();
          }
        });
    token = fooUserClient.getDelegationToken(httpUser.getShortUserName());
    assertNotNull(token, "Delegation token should not be null");
    tDT = token.decodeIdentifier();
    assertNotNull(tDT,
        "Delegation token identifier should not be null");
    assertEquals(new Text(FOO_USER), tDT.getOwner(),
        "Owner of delegation token is not the expected");
    assertEquals(new Text(HTTP_USER), tDT.getRealUser(),
        "Real user of delegation token is not the expected");

    // Renew token as the renewer
    final Token<TimelineDelegationTokenIdentifier> tokenToRenew = token;
    renewTime1 = httpUserClient.renewDelegationToken(tokenToRenew);
    renewTime2 = httpUserClient.renewDelegationToken(tokenToRenew);
    assertTrue(renewTime1 < renewTime2,
        "renewTime2 should be later than renewTime1");

    // Cancel token
    assertFalse(tokenToRenew.getService().toString().isEmpty(),
        "Service field of token should not be empty");
    // Cancel the token from the token service address
    fooUserClient.cancelDelegationToken(tokenToRenew);

    // Renew should not be successful because the token is canceled
    try {
      httpUserClient.renewDelegationToken(tokenToRenew);
      fail("Renew of delegation token should not be successful");
    } catch (Exception e) {
      LOG.info("Exception while renewing delegation token", e);
      assertTrue(
          e.getMessage().contains("Renewal request for unknown token"));
    }

    // Let HTTP user to get the delegation token for BAR user
    UserGroupInformation barUgi = UserGroupInformation.createProxyUser(
        BAR_USER, httpUser);
    TimelineClient barUserClient = barUgi.doAs(
        new PrivilegedExceptionAction<TimelineClient>() {
          @Override
          public TimelineClient run() {
            return createTimelineClientForUGI();
          }
        });

    try {
      barUserClient.getDelegationToken(httpUser.getShortUserName());
      fail("Retrieval of delegation token should not be successful");
    } catch (Exception e) {
      LOG.info("Exception while retrieving delegation token", e);
      assertTrue(e.getCause() instanceof AuthorizationException ||
          e.getCause() instanceof AuthenticationException);
    }
  }
}

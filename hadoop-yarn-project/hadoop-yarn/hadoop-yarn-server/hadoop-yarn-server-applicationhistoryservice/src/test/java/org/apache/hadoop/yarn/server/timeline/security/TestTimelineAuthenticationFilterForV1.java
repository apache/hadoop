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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test cases for authentication via TimelineAuthenticationFilter while
 * publishing entities for ATSv1.
 */
@RunWith(Parameterized.class)
public class TestTimelineAuthenticationFilterForV1 {

  private static final String FOO_USER = "foo";
  private static final String BAR_USER = "bar";
  private static final String HTTP_USER = "HTTP";

  private static final File TEST_ROOT_DIR = new File(
      System.getProperty("test.build.dir", "target/test-dir"),
          TestTimelineAuthenticationFilterForV1.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());
  private static String httpSpnegoPrincipal =
      KerberosTestUtils.getServerPrincipal();
  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/"
          + TestTimelineAuthenticationFilterForV1.class.getSimpleName();

  @Parameterized.Parameters
  public static Collection<Object[]> withSsl() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  private static MiniKdc testMiniKDC;
  private static String keystoresDir;
  private static String sslConfDir;
  private static ApplicationHistoryServer testTimelineServer;
  private static Configuration conf;
  private static boolean withSsl;

  public TestTimelineAuthenticationFilterForV1(boolean withSsl) {
    TestTimelineAuthenticationFilterForV1.withSsl = withSsl;
  }

  @BeforeClass
  public static void setup() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), TEST_ROOT_DIR);
      testMiniKDC.start();
      testMiniKDC.createPrincipal(
          httpSpnegoKeytabFile, HTTP_USER + "/localhost");
    } catch (Exception e) {
      assertTrue("Couldn't setup MiniKDC", false);
    }

    try {
      testTimelineServer = new ApplicationHistoryServer();
      conf = new Configuration(false);
      conf.setStrings(TimelineAuthenticationFilterInitializer.PREFIX + "type",
          "kerberos");
      conf.set(TimelineAuthenticationFilterInitializer.PREFIX +
          KerberosAuthenticationHandler.PRINCIPAL, httpSpnegoPrincipal);
      conf.set(TimelineAuthenticationFilterInitializer.PREFIX +
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
      e.printStackTrace();
      assertTrue("Couldn't setup TimelineServer", false);
    }
  }

  private TimelineClient createTimelineClientForUGI() {
    TimelineClient client = TimelineClient.createTimelineClient();
    client.init(conf);
    client.start();
    return client;
  }

  @AfterClass
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

  @Test
  public void testPutTimelineEntities() throws Exception {
    KerberosTestUtils.doAs(HTTP_USER + "/localhost", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        TimelineClient client = createTimelineClientForUGI();
        TimelineEntity entityToStore = new TimelineEntity();
        entityToStore.setEntityType(
            TestTimelineAuthenticationFilterForV1.class.getName());
        entityToStore.setEntityId("entity1");
        entityToStore.setStartTime(0L);
        TimelinePutResponse putResponse = client.putEntities(entityToStore);
        Assert.assertEquals(0, putResponse.getErrors().size());
        TimelineEntity entityToRead =
            testTimelineServer.getTimelineStore().getEntity("entity1",
                TestTimelineAuthenticationFilterForV1.class.getName(), null);
        Assert.assertNotNull(entityToRead);
        return null;
      }
    });
  }

  @Test
  public void testPutDomains() throws Exception {
    KerberosTestUtils.doAs(HTTP_USER + "/localhost", new Callable<Void>() {
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
        Assert.assertNotNull(domainToRead);
        return null;
      }
    });
  }

  @Test
  public void testDelegationTokenOperations() throws Exception {
    TimelineClient httpUserClient =
        KerberosTestUtils.doAs(HTTP_USER + "/localhost",
            new Callable<TimelineClient>() {
            @Override
            public TimelineClient call() throws Exception {
              return createTimelineClientForUGI();
            }
          });
    UserGroupInformation httpUser =
        KerberosTestUtils.doAs(HTTP_USER + "/localhost",
            new Callable<UserGroupInformation>() {
            @Override
            public UserGroupInformation call() throws Exception {
              return UserGroupInformation.getCurrentUser();
            }
          });
    // Let HTTP user to get the delegation for itself
    Token<TimelineDelegationTokenIdentifier> token =
        httpUserClient.getDelegationToken(httpUser.getShortUserName());
    Assert.assertNotNull(token);
    TimelineDelegationTokenIdentifier tDT = token.decodeIdentifier();
    Assert.assertNotNull(tDT);
    Assert.assertEquals(new Text(HTTP_USER), tDT.getOwner());

    // Renew token
    Assert.assertFalse(token.getService().toString().isEmpty());
    // Renew the token from the token service address
    long renewTime1 = httpUserClient.renewDelegationToken(token);
    Thread.sleep(100);
    token.setService(new Text());
    Assert.assertTrue(token.getService().toString().isEmpty());
    // If the token service address is not avaiable, it still can be renewed
    // from the configured address
    long renewTime2 = httpUserClient.renewDelegationToken(token);
    Assert.assertTrue(renewTime1 < renewTime2);

    // Cancel token
    Assert.assertTrue(token.getService().toString().isEmpty());
    // If the token service address is not avaiable, it still can be canceled
    // from the configured address
    httpUserClient.cancelDelegationToken(token);
    // Renew should not be successful because the token is canceled
    try {
      httpUserClient.renewDelegationToken(token);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(
            "Renewal request for unknown token"));
    }

    // Let HTTP user to get the delegation token for FOO user
    UserGroupInformation fooUgi = UserGroupInformation.createProxyUser(
        FOO_USER, httpUser);
    TimelineClient fooUserClient = fooUgi.doAs(
        new PrivilegedExceptionAction<TimelineClient>() {
          @Override
          public TimelineClient run() throws Exception {
            return createTimelineClientForUGI();
          }
        });
    token = fooUserClient.getDelegationToken(httpUser.getShortUserName());
    Assert.assertNotNull(token);
    tDT = token.decodeIdentifier();
    Assert.assertNotNull(tDT);
    Assert.assertEquals(new Text(FOO_USER), tDT.getOwner());
    Assert.assertEquals(new Text(HTTP_USER), tDT.getRealUser());

    // Renew token as the renewer
    final Token<TimelineDelegationTokenIdentifier> tokenToRenew = token;
    renewTime1 = httpUserClient.renewDelegationToken(tokenToRenew);
    renewTime2 = httpUserClient.renewDelegationToken(tokenToRenew);
    Assert.assertTrue(renewTime1 < renewTime2);

    // Cancel token
    Assert.assertFalse(tokenToRenew.getService().toString().isEmpty());
    // Cancel the token from the token service address
    fooUserClient.cancelDelegationToken(tokenToRenew);

    // Renew should not be successful because the token is canceled
    try {
      httpUserClient.renewDelegationToken(tokenToRenew);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(
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
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof AuthorizationException ||
          e.getCause() instanceof AuthenticationException);
    }
  }
}

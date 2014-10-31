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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTimelineAuthenticationFilter {

  private static final String FOO_USER = "foo";
  private static final String BAR_USER = "bar";
  private static final String HTTP_USER = "HTTP";

  private static final File testRootDir = new File(
      System.getProperty("test.build.dir", "target/test-dir"),
      TestTimelineAuthenticationFilter.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());
  private static String httpSpnegoPrincipal =
      KerberosTestUtils.getServerPrincipal();
  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/"
          + TestTimelineAuthenticationFilter.class.getSimpleName();

  @Parameterized.Parameters
  public static Collection<Object[]> withSsl() {
    return Arrays.asList(new Object[][] { { false }, { true } });
  }

  private MiniKdc testMiniKDC;
  private String keystoresDir;
  private String sslConfDir;
  private ApplicationHistoryServer testTimelineServer;
  private Configuration conf;
  private TimelineClient client;
  private boolean withSsl;

  public TestTimelineAuthenticationFilter(boolean withSsl) {
    this.withSsl = withSsl;
  }

  @Before
  public void setup() {
    try {
      testMiniKDC = new MiniKdc(MiniKdc.createConf(), testRootDir);
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

      if (withSsl) {
        conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY,
            HttpConfig.Policy.HTTPS_ONLY.name());
        File base = new File(BASEDIR);
        FileUtil.fullyDelete(base);
        base.mkdirs();
        keystoresDir = new File(BASEDIR).getAbsolutePath();
        sslConfDir =
            KeyStoreTestUtil.getClasspathDir(TestTimelineAuthenticationFilter.class);
        KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
      }

      UserGroupInformation.setConfiguration(conf);
      testTimelineServer.init(conf);
      testTimelineServer.start();
    } catch (Exception e) {
      assertTrue("Couldn't setup TimelineServer", false);
    }

    client = TimelineClient.createTimelineClient();
    client.init(conf);
    client.start();
  }

  @After
  public void tearDown() throws Exception {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }

    if (testTimelineServer != null) {
      testTimelineServer.stop();
    }

    if (client != null) {
      client.stop();
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
        TimelineEntity entityToStore = new TimelineEntity();
        entityToStore.setEntityType(
            TestTimelineAuthenticationFilter.class.getName());
        entityToStore.setEntityId("entity1");
        entityToStore.setStartTime(0L);
        TimelinePutResponse putResponse = client.putEntities(entityToStore);
        Assert.assertEquals(0, putResponse.getErrors().size());
        TimelineEntity entityToRead =
            testTimelineServer.getTimelineStore().getEntity(
                "entity1", TestTimelineAuthenticationFilter.class.getName(), null);
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
        TimelineDomain domainToStore = new TimelineDomain();
        domainToStore.setId(TestTimelineAuthenticationFilter.class.getName());
        domainToStore.setReaders("*");
        domainToStore.setWriters("*");
        client.putDomain(domainToStore);
        TimelineDomain domainToRead =
            testTimelineServer.getTimelineStore().getDomain(
                TestTimelineAuthenticationFilter.class.getName());
        Assert.assertNotNull(domainToRead);
        return null;
      }
    });
  }

  @Test
  public void testDelegationTokenOperations() throws Exception {
    KerberosTestUtils.doAs(HTTP_USER + "/localhost", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // Let HTTP user to get the delegation for itself
        Token<TimelineDelegationTokenIdentifier> token =
            client.getDelegationToken(
                UserGroupInformation.getCurrentUser().getShortUserName());
        Assert.assertNotNull(token);
        TimelineDelegationTokenIdentifier tDT = token.decodeIdentifier();
        Assert.assertNotNull(tDT);
        Assert.assertEquals(new Text(HTTP_USER), tDT.getOwner());

        // Renew token
        long renewTime1 = client.renewDelegationToken(token);
        Thread.sleep(100);
        long renewTime2 = client.renewDelegationToken(token);
        Assert.assertTrue(renewTime1 < renewTime2);

        // Cancel token
        client.cancelDelegationToken(token);
        // Renew should not be successful because the token is canceled
        try {
          client.renewDelegationToken(token);
          Assert.fail();
        } catch (Exception e) {
          Assert.assertTrue(e.getMessage().contains(
              "Renewal request for unknown token"));
        }

        // Let HTTP user to get the delegation token for FOO user
        UserGroupInformation fooUgi = UserGroupInformation.createProxyUser(
            FOO_USER, UserGroupInformation.getCurrentUser());
        token = fooUgi.doAs(
            new PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>>() {
          @Override
          public Token<TimelineDelegationTokenIdentifier> run()
              throws Exception {
            return client.getDelegationToken(
                UserGroupInformation.getCurrentUser().getShortUserName());
          }
        });
        Assert.assertNotNull(token);
        tDT = token.decodeIdentifier();
        Assert.assertNotNull(tDT);
        Assert.assertEquals(new Text(FOO_USER), tDT.getOwner());
        Assert.assertEquals(new Text(HTTP_USER), tDT.getRealUser());

        // Renew token
        final Token<TimelineDelegationTokenIdentifier> tokenToRenew = token;
        renewTime1 = fooUgi.doAs(
            new PrivilegedExceptionAction<Long>() {
          @Override
          public Long run() throws Exception {
            return client.renewDelegationToken(tokenToRenew);
          }
        });
        renewTime2 = fooUgi.doAs(
            new PrivilegedExceptionAction<Long>() {
          @Override
          public Long run() throws Exception {
            return client.renewDelegationToken(tokenToRenew);
          }
        });
        Assert.assertTrue(renewTime1 < renewTime2);

        // Cancel token
        fooUgi.doAs(
            new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            client.cancelDelegationToken(tokenToRenew);
            return null;
          }
        });
        // Renew should not be successful because the token is canceled
        try {
          fooUgi.doAs(
              new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              client.renewDelegationToken(tokenToRenew);
              return null;
            }
          });
          Assert.fail();
        } catch (Exception e) {
          Assert.assertTrue(e.getMessage().contains(
              "Renewal request for unknown token"));
        }

        // Let HTTP user to get the delegation token for BAR user
        UserGroupInformation barUgi = UserGroupInformation.createProxyUser(
            BAR_USER, UserGroupInformation.getCurrentUser());
        token = barUgi.doAs(
            new PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>>() {
          @Override
          public Token<TimelineDelegationTokenIdentifier> run()
              throws Exception {
            try {
              Token<TimelineDelegationTokenIdentifier> token =
                  client.getDelegationToken(
                      UserGroupInformation.getCurrentUser().getShortUserName());
              Assert.fail();
              return token;
            } catch (Exception e) {
              Assert.assertTrue(e instanceof AuthorizationException);
              return null;
            }
          }
        });
        return null;
      }
    });
  }

}

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
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.KerberosTestUtils;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimelineAuthenticationFilter {

  private static final String FOO_USER = "foo";
  private static final String BAR_USER = "bar";
  private static final String HTTP_USER = "HTTP";

  private static final File testRootDir = new File("target",
      TestTimelineAuthenticationFilter.class.getName() + "-root");
  private static File httpSpnegoKeytabFile = new File(
      KerberosTestUtils.getKeytabFile());
  private static String httpSpnegoPrincipal =
      KerberosTestUtils.getServerPrincipal();
  private static MiniKdc testMiniKDC;
  private static ApplicationHistoryServer testTimelineServer;
  private static Configuration conf;

  @BeforeClass
  public static void setupClass() {
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
      conf = new YarnConfiguration();
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
      conf.set("hadoop.proxyuser.HTTP.hosts", "*");
      conf.set("hadoop.proxyuser.HTTP.users", FOO_USER);
      UserGroupInformation.setConfiguration(conf);
      testTimelineServer.init(conf);
      testTimelineServer.start();
    } catch (Exception e) {
      assertTrue("Couldn't setup TimelineServer", false);
    }
  }

  @AfterClass
  public static void tearDownClass() {
    if (testMiniKDC != null) {
      testMiniKDC.stop();
    }

    if (testTimelineServer != null) {
      testTimelineServer.stop();
    }
  }

  private TimelineClient client;

  @Before
  public void setup() throws Exception {
    client = TimelineClient.createTimelineClient();
    client.init(conf);
    client.start();
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      client.stop();
    }
  }

  @Test
  public void testPutTimelineEntities() throws Exception {
    KerberosTestUtils.doAs(HTTP_USER + "/localhost", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        TimelineEntity entityToStore = new TimelineEntity();
        entityToStore.setEntityType("TestTimelineAuthenticationFilter");
        entityToStore.setEntityId("entity1");
        entityToStore.setStartTime(0L);
        TimelinePutResponse putResponse = client.putEntities(entityToStore);
        Assert.assertEquals(0, putResponse.getErrors().size());
        TimelineEntity entityToRead =
            testTimelineServer.getTimelineStore().getEntity(
                "entity1", "TestTimelineAuthenticationFilter", null);
        Assert.assertNotNull(entityToRead);
        return null;
      }
    });
  }

  @Test
  public void testGetDelegationToken() throws Exception {
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

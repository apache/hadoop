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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.test.TestGenericTestUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_MONITORING_THREAD_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTimelineClient {

  private TimelineClientImpl client;
  private TimelineWriter spyTimelineWriter;
  private String keystoresDir;
  private String sslConfDir;

  @BeforeEach
  public void setup() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.0f);
    client = createTimelineClient(conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (client != null) {
      client.stop();
    }
    if (isSSLConfigured()) {
      KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
    }
  }

  @Test
  void testPostEntities() throws Exception {
    mockEntityClientResponse(spyTimelineWriter, ClientResponse.Status.OK,
        false, false);
    try {
      TimelinePutResponse response = client.putEntities(generateEntity());
      assertEquals(0, response.getErrors().size());
    } catch (YarnException e) {
      fail("Exception is not expected");
    }
  }

  @Test
  void testPostEntitiesWithError() throws Exception {
    mockEntityClientResponse(spyTimelineWriter, ClientResponse.Status.OK, true,
        false);
    try {
      TimelinePutResponse response = client.putEntities(generateEntity());
      assertEquals(1, response.getErrors().size());
      assertEquals("test entity id", response.getErrors().get(0)
          .getEntityId());
      assertEquals("test entity type", response.getErrors().get(0)
          .getEntityType());
      assertEquals(TimelinePutResponse.TimelinePutError.IO_EXCEPTION,
          response.getErrors().get(0).getErrorCode());
    } catch (YarnException e) {
      fail("Exception is not expected");
    }
  }

  @Test
  void testPostIncompleteEntities() throws Exception {
    try {
      client.putEntities(new TimelineEntity());
      fail("Exception should have been thrown");
    } catch (YarnException e) {
    }
  }

  @Test
  void testPostEntitiesNoResponse() throws Exception {
    mockEntityClientResponse(spyTimelineWriter,
        ClientResponse.Status.INTERNAL_SERVER_ERROR, false, false);
    try {
      client.putEntities(generateEntity());
      fail("Exception is expected");
    } catch (YarnException e) {
      assertTrue(e.getMessage().contains(
          "Failed to get the response from the timeline server."));
    }
  }

  @Test
  void testPostEntitiesConnectionRefused() throws Exception {
    mockEntityClientResponse(spyTimelineWriter, null, false, true);
    try {
      client.putEntities(generateEntity());
      fail("RuntimeException is expected");
    } catch (RuntimeException re) {
      assertTrue(re instanceof ClientHandlerException);
    }
  }

  @Test
  void testPutDomain() throws Exception {
    mockDomainClientResponse(spyTimelineWriter, ClientResponse.Status.OK, false);
    try {
      client.putDomain(generateDomain());
    } catch (YarnException e) {
      fail("Exception is not expected");
    }
  }

  @Test
  void testPutDomainNoResponse() throws Exception {
    mockDomainClientResponse(spyTimelineWriter,
        ClientResponse.Status.FORBIDDEN, false);
    try {
      client.putDomain(generateDomain());
      fail("Exception is expected");
    } catch (YarnException e) {
      assertTrue(e.getMessage().contains(
          "Failed to get the response from the timeline server."));
    }
  }

  @Test
  void testPutDomainConnectionRefused() throws Exception {
    mockDomainClientResponse(spyTimelineWriter, null, true);
    try {
      client.putDomain(generateDomain());
      fail("RuntimeException is expected");
    } catch (RuntimeException re) {
      assertTrue(re instanceof ClientHandlerException);
    }
  }

  @Test
  void testCheckRetryCount() throws Exception {
    try {
      YarnConfiguration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
          -2);
      createTimelineClient(conf);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES));
    }

    try {
      YarnConfiguration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setLong(YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
          0);
      createTimelineClient(conf);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS));
    }
    int newMaxRetries = 5;
    long newIntervalMs = 500;
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
        newMaxRetries);
    conf.setLong(YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
        newIntervalMs);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    TimelineClientImpl client = createTimelineClient(conf);
    try {
      // This call should fail because there is no timeline server
      client.putEntities(generateEntity());
      fail("Exception expected! "
          + "Timeline server should be off to run this test. ");
    } catch (RuntimeException ce) {
      assertTrue(
          ce.getMessage().contains("Connection retries limit exceeded"),
          "Handler exception for reason other than retry: " + ce.getMessage());
      // we would expect this exception here, check if the client has retried
      assertTrue(client.connector.connectionRetry.getRetired(),
          "Retry filter didn't perform any retries! ");
    }
  }

  @Test
  void testDelegationTokenOperationsRetry() throws Exception {
    int newMaxRetries = 5;
    long newIntervalMs = 500;
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
        newMaxRetries);
    conf.setLong(YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
        newIntervalMs);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    // use kerberos to bypass the issue in HADOOP-11215
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set(YarnConfiguration.TIMELINE_HTTP_AUTH_TYPE, "kerberos");
    UserGroupInformation.setConfiguration(conf);

    TimelineClientImpl client = createTimelineClient(conf);
    TimelineClientImpl clientFake =
        createTimelineClientFakeTimelineClientRetryOp(conf);
    TestTimelineDelegationTokenSecretManager dtManager =
        new TestTimelineDelegationTokenSecretManager();
    try {
      dtManager.startThreads();
      Thread.sleep(3000);

      try {
        // try getting a delegation token
        client.getDelegationToken(
            UserGroupInformation.getCurrentUser().getShortUserName());
        assertFail();
      } catch (RuntimeException ce) {
        assertException(client, ce);
      }

      try {
        // try renew a delegation token
        TimelineDelegationTokenIdentifier timelineDT =
            new TimelineDelegationTokenIdentifier(
                new Text("tester"), new Text("tester"), new Text("tester"));
        client.renewDelegationToken(
            new Token<TimelineDelegationTokenIdentifier>(timelineDT.getBytes(),
                dtManager.createPassword(timelineDT),
                timelineDT.getKind(),
                new Text("0.0.0.0:8188")));
        assertFail();
      } catch (RuntimeException ce) {
        assertException(client, ce);
      }

      try {
        // try cancel a delegation token
        TimelineDelegationTokenIdentifier timelineDT =
            new TimelineDelegationTokenIdentifier(
                new Text("tester"), new Text("tester"), new Text("tester"));
        client.cancelDelegationToken(
            new Token<TimelineDelegationTokenIdentifier>(timelineDT.getBytes(),
                dtManager.createPassword(timelineDT),
                timelineDT.getKind(),
                new Text("0.0.0.0:8188")));
        assertFail();
      } catch (RuntimeException ce) {
        assertException(client, ce);
      }

      // Test DelegationTokenOperationsRetry on SocketTimeoutException
      try {
        TimelineDelegationTokenIdentifier timelineDT =
            new TimelineDelegationTokenIdentifier(
                new Text("tester"), new Text("tester"), new Text("tester"));
        clientFake.cancelDelegationToken(
            new Token<TimelineDelegationTokenIdentifier>(timelineDT.getBytes(),
                dtManager.createPassword(timelineDT),
                timelineDT.getKind(),
                new Text("0.0.0.0:8188")));
        assertFail();
      } catch (RuntimeException ce) {
        assertException(clientFake, ce);
      }
    } finally {
      client.stop();
      clientFake.stop();
      dtManager.stopThreads();
    }
  }

  /**
   * Test actual delegation token operations are not carried out when
   * simple auth is configured for timeline.
   * @throws Exception
   */
  @Test
  void testDelegationTokenDisabledOnSimpleAuth() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.set(YarnConfiguration.TIMELINE_HTTP_AUTH_TYPE, "simple");
    UserGroupInformation.setConfiguration(conf);

    TimelineClientImpl tClient = createTimelineClient(conf);
    TimelineConnector spyConnector = spy(tClient.connector);
    tClient.connector = spyConnector;
    try {
      // try getting a delegation token
      Token<TimelineDelegationTokenIdentifier> identifierToken =
          tClient.getDelegationToken(
              UserGroupInformation.getCurrentUser().getShortUserName());
      // Get a null token when using simple auth
      assertNull(identifierToken);

      // try renew a delegation token
      Token<TimelineDelegationTokenIdentifier> dummyToken = new Token<>();
      long renewTime = tClient.renewDelegationToken(dummyToken);
      // Get invalid expiration time so that RM skips renewal
      assertEquals(renewTime, -1);

      // try cancel a delegation token
      tClient.cancelDelegationToken(dummyToken);
      // Shouldn't try to cancel and connect to authURL
      verify(spyConnector, never()).getDelegationTokenAuthenticatedURL();
    } finally {
      tClient.stop();
    }
  }

  private static void assertFail() {
    fail("Exception expected! "
        + "Timeline server should be off to run this test.");
  }

  private void assertException(TimelineClientImpl client, RuntimeException ce) {
    assertTrue(ce.getMessage().contains("Connection retries limit exceeded"),
        "Handler exception for reason other than retry: " + ce.toString());
    // we would expect this exception here, check if the client has retried
    assertTrue(client.connector.connectionRetry.getRetired(),
        "Retry filter didn't perform any retries! ");
  }

  public static ClientResponse mockEntityClientResponse(
      TimelineWriter spyTimelineWriter, ClientResponse.Status status,
      boolean hasError, boolean hasRuntimeError) {
    ClientResponse response = mock(ClientResponse.class);
    if (hasRuntimeError) {
      doThrow(new ClientHandlerException(new ConnectException())).when(
          spyTimelineWriter).doPostingObject(
              any(TimelineEntities.class), any());
      return response;
    }
    doReturn(response).when(spyTimelineWriter)
        .doPostingObject(any(TimelineEntities.class), any());
    when(response.getStatusInfo()).thenReturn(status);
    TimelinePutResponse.TimelinePutError error =
        new TimelinePutResponse.TimelinePutError();
    error.setEntityId("test entity id");
    error.setEntityType("test entity type");
    error.setErrorCode(TimelinePutResponse.TimelinePutError.IO_EXCEPTION);
    TimelinePutResponse putResponse = new TimelinePutResponse();
    if (hasError) {
      putResponse.addError(error);
    }
    when(response.getEntity(TimelinePutResponse.class)).thenReturn(putResponse);
    return response;
  }

  private static ClientResponse mockDomainClientResponse(
      TimelineWriter spyTimelineWriter, ClientResponse.Status status,
      boolean hasRuntimeError) {
    ClientResponse response = mock(ClientResponse.class);
    if (hasRuntimeError) {
      doThrow(new ClientHandlerException(new ConnectException())).when(
        spyTimelineWriter).doPostingObject(any(TimelineDomain.class),
        any(String.class));
      return response;
    }
    doReturn(response).when(spyTimelineWriter)
        .doPostingObject(any(TimelineDomain.class), any(String.class));
    when(response.getStatusInfo()).thenReturn(status);
    return response;
  }

  private static TimelineEntity generateEntity() {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("entity id");
    entity.setEntityType("entity type");
    entity.setStartTime(System.currentTimeMillis());
    for (int i = 0; i < 2; ++i) {
      TimelineEvent event = new TimelineEvent();
      event.setTimestamp(System.currentTimeMillis());
      event.setEventType("test event type " + i);
      event.addEventInfo("key1", "val1");
      event.addEventInfo("key2", "val2");
      entity.addEvent(event);
    }
    entity.addRelatedEntity("test ref type 1", "test ref id 1");
    entity.addRelatedEntity("test ref type 2", "test ref id 2");
    entity.addPrimaryFilter("pkey1", "pval1");
    entity.addPrimaryFilter("pkey2", "pval2");
    entity.addOtherInfo("okey1", "oval1");
    entity.addOtherInfo("okey2", "oval2");
    entity.setDomainId("domain id 1");
    return entity;
  }

  public static TimelineDomain generateDomain() {
    TimelineDomain domain = new TimelineDomain();
    domain.setId("namespace id");
    domain.setDescription("domain description");
    domain.setOwner("domain owner");
    domain.setReaders("domain_reader");
    domain.setWriters("domain_writer");
    domain.setCreatedTime(0L);
    domain.setModifiedTime(1L);
    return domain;
  }

  private TimelineClientImpl createTimelineClient(
      YarnConfiguration conf) {
    TimelineClientImpl client = new TimelineClientImpl() {
      @Override
      protected TimelineWriter createTimelineWriter(Configuration conf,
          UserGroupInformation authUgi, Client client, URI resURI)
          throws IOException {
        TimelineWriter timelineWriter =
            new DirectTimelineWriter(authUgi, client, resURI);
        spyTimelineWriter = spy(timelineWriter);
        return spyTimelineWriter;
      }
    };
    client.init(conf);
    client.start();
    return client;
  }

  private TimelineClientImpl createTimelineClientFakeTimelineClientRetryOp(
      YarnConfiguration conf) {
    TimelineClientImpl client = new TimelineClientImpl() {
      @Override
      protected TimelineConnector createTimelineConnector() {
        TimelineConnector connector =
            new TimelineConnector(true, authUgi, doAsUser, token) {
              @Override
              public TimelineClientRetryOp
                createRetryOpForOperateDelegationToken(
                  final PrivilegedExceptionAction<?> action)
                  throws IOException {
                TimelineClientRetryOpForOperateDelegationToken op =
                    spy(new TimelineClientRetryOpForOperateDelegationToken(
                        UserGroupInformation.getCurrentUser(), action));
                doThrow(
                    new SocketTimeoutException("Test socketTimeoutException"))
                        .when(op).run();
                return op;
              }
            };
        addIfService(connector);
        return connector;
      }
    };
    client.init(conf);
    client.start();
    return client;
  }

  @Test
  void testTimelineClientCleanup() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES, 0);
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, Policy.HTTPS_ONLY.name());

    setupSSLConfig(conf);
    client = createTimelineClient(conf);

    ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();

    while (threadGroup.getParent() != null) {
      threadGroup = threadGroup.getParent();
    }

    Thread[] threads = new Thread[threadGroup.activeCount()];

    threadGroup.enumerate(threads);
    Thread reloaderThread = null;
    for (Thread thread : threads) {
      if ((thread.getName() != null)
          && (thread.getName().contains(SSL_MONITORING_THREAD_NAME))) {
        reloaderThread = thread;
      }
    }
    assertTrue(reloaderThread.isAlive(), "Reloader is not alive");

    client.close();

    boolean reloaderStillAlive = true;
    for (int i = 0; i < 10; i++) {
      reloaderStillAlive = reloaderThread.isAlive();
      if (!reloaderStillAlive) {
        break;
      }
      Thread.sleep(1000);
    }
    assertFalse(reloaderStillAlive, "Reloader is still alive");
  }

  @Test
  void testTimelineConnectorDestroy() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    TimelineClientImpl client = createTimelineClient(conf);
    Client mockJerseyClient = mock(Client.class);
    client.connector.client = mockJerseyClient;
    client.stop();
    verify(mockJerseyClient, times(1)).destroy();
  }

  private void setupSSLConfig(YarnConfiguration conf) throws Exception {
    keystoresDir = TestGenericTestUtils.getTestDir().getAbsolutePath();
    sslConfDir =
        KeyStoreTestUtil.getClasspathDir(TestTimelineClient.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
  }

  private boolean isSSLConfigured() {
    return keystoresDir != null && sslConfDir != null;
  }

  private static class TestTimelineDelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    public TestTimelineDelegationTokenSecretManager() {
      super(100000, 100000, 100000, 100000);
    }

    @Override
    public TimelineDelegationTokenIdentifier createIdentifier() {
      return new TimelineDelegationTokenIdentifier();
    }

    @Override
    public synchronized byte[] createPassword(TimelineDelegationTokenIdentifier identifier) {
      return super.createPassword(identifier);
    }
  }
}

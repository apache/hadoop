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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.MultivaluedMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTimelineClientV2Impl {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTimelineClientV2Impl.class);
  private TestV2TimelineClient client;
  private static final long TIME_TO_SLEEP = 150L;
  private static final String EXCEPTION_MSG = "Exception in the content";

  @BeforeEach
  public void setup(TestInfo testInfo) {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.setInt(YarnConfiguration.NUMBER_OF_ASYNC_ENTITIES_TO_MERGE, 3);
    if (!testInfo.getDisplayName()
        .contains("testRetryOnConnectionFailure")) {
      client = createTimelineClient(conf);
    }
  }

  @Test
  void getTestInfo(TestInfo testInfo) {
    System.out.println(testInfo.getDisplayName());
    System.out.println(testInfo.getTestMethod());
    System.out.println(testInfo.getTestClass());
    System.out.println(testInfo.getTags());
  }
  private YarnConfiguration conf;

  private TestV2TimelineClient createTimelineClient(YarnConfiguration config) {
    ApplicationId id = ApplicationId.newInstance(0, 0);
    TestV2TimelineClient tc = new TestV2TimelineClient(id);
    tc.init(config);
    tc.start();
    return tc;
  }

  private class TestV2TimelineClientForExceptionHandling
      extends TimelineV2ClientImpl {
    public TestV2TimelineClientForExceptionHandling(ApplicationId id) {
      super(id);
    }

    private boolean throwYarnException;

    public void setThrowYarnException(boolean throwYarnException) {
      this.throwYarnException = throwYarnException;
    }

    public boolean isThrowYarnException() {
      return throwYarnException;
    }

    @Override
    protected void putObjects(URI base, String path,
        MultivaluedMap<String, String> params, Object obj)
            throws IOException, YarnException {
      if (throwYarnException) {
        throw new YarnException(EXCEPTION_MSG);
      } else {
        throw new IOException(
            "Failed to get the response from the timeline server.");
      }
    }
  }

  private class TestV2TimelineClient
      extends TestV2TimelineClientForExceptionHandling {
    private boolean sleepBeforeReturn;

    private List<TimelineEntities> publishedEntities;

    public TimelineEntities getPublishedEntities(int putIndex) {
      assertTrue(putIndex < publishedEntities.size(),
          "Not So many entities Published");
      return publishedEntities.get(putIndex);
    }

    public void setSleepBeforeReturn(boolean sleepBeforeReturn) {
      this.sleepBeforeReturn = sleepBeforeReturn;
    }

    public int getNumOfTimelineEntitiesPublished() {
      return publishedEntities.size();
    }

    public TestV2TimelineClient(ApplicationId id) {
      super(id);
      publishedEntities = new ArrayList<TimelineEntities>();
    }

    protected void putObjects(String path,
        MultivaluedMap<String, String> params, Object obj)
            throws IOException, YarnException {
      if (isThrowYarnException()) {
        throw new YarnException("ActualException");
      }
      publishedEntities.add((TimelineEntities) obj);
      if (sleepBeforeReturn) {
        try {
          Thread.sleep(TIME_TO_SLEEP);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Test
  void testExceptionMultipleRetry() {
    TestV2TimelineClientForExceptionHandling c =
        new TestV2TimelineClientForExceptionHandling(
            ApplicationId.newInstance(0, 0));
    int maxRetries = 2;
    conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
        maxRetries);
    c.init(conf);
    c.start();
    c.setTimelineCollectorInfo(CollectorInfo.newInstance("localhost:12345"));
    try {
      c.putEntities(new TimelineEntity());
    } catch (IOException e) {
      fail("YARN exception is expected");
    } catch (YarnException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IOException,
          "IOException is expected");
      assertTrue(cause.getMessage().contains(
              "TimelineClient has reached to max retry times : " + maxRetries),
          "YARN exception is expected");
    }

    c.setThrowYarnException(true);
    try {
      c.putEntities(new TimelineEntity());
    } catch (IOException e) {
      fail("YARN exception is expected");
    } catch (YarnException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof YarnException,
          "YARN exception is expected");
      assertTrue(cause.getMessage().contains(EXCEPTION_MSG),
          "YARN exception is expected");
    }
    c.stop();
  }

  @Test
  void testPostEntities() throws Exception {
    try {
      client.putEntities(generateEntity("1"));
    } catch (YarnException e) {
      fail("Exception is not expected");
    }
  }

  @Test
  void testASyncCallMerge() throws Exception {
    client.setSleepBeforeReturn(true);
    try {
      client.putEntitiesAsync(generateEntity("1"));
      Thread.sleep(TIME_TO_SLEEP / 2);
      // by the time first put response comes push 2 entities in the queue
      client.putEntitiesAsync(generateEntity("2"));
      client.putEntitiesAsync(generateEntity("3"));
    } catch (YarnException e) {
      fail("Exception is not expected");
    }
    for (int i = 0; i < 4; i++) {
      if (client.getNumOfTimelineEntitiesPublished() == 2) {
        break;
      }
      Thread.sleep(TIME_TO_SLEEP);
    }
    assertEquals(2,
        client.getNumOfTimelineEntitiesPublished(),
        "two merged TimelineEntities needs to be published");
    TimelineEntities secondPublishedEntities = client.getPublishedEntities(1);
    assertEquals(
        2,
        secondPublishedEntities.getEntities().size(),
        "Merged TimelineEntities Object needs to 2 TimelineEntity Object");
    assertEquals("2",
        secondPublishedEntities.getEntities().get(0).getId(),
        "Order of Async Events Needs to be FIFO");
    assertEquals("3",
        secondPublishedEntities.getEntities().get(1).getId(),
        "Order of Async Events Needs to be FIFO");
  }

  @Test
  void testSyncCall() throws Exception {
    try {
      // sync entity should not be merged with Async
      client.putEntities(generateEntity("1"));
      client.putEntitiesAsync(generateEntity("2"));
      client.putEntitiesAsync(generateEntity("3"));
      // except for the sync call above 2 should be merged
      client.putEntities(generateEntity("4"));
    } catch (YarnException e) {
      fail("Exception is not expected");
    }
    for (int i = 0; i < 4; i++) {
      if (client.getNumOfTimelineEntitiesPublished() == 3) {
        break;
      }
      Thread.sleep(TIME_TO_SLEEP);
    }
    printReceivedEntities();

    boolean asyncPushesMerged = client.getNumOfTimelineEntitiesPublished() == 3;
    int lastPublishIndex = asyncPushesMerged ? 2 : 3;

    TimelineEntities firstPublishedEntities = client.getPublishedEntities(0);
    assertEquals(1,
        firstPublishedEntities.getEntities().size(),
        "sync entities should not be merged with async");

    // async push does not guarantee a merge but is FIFO
    if (asyncPushesMerged) {
      TimelineEntities secondPublishedEntities = client.getPublishedEntities(1);
      assertEquals(
          2,
          secondPublishedEntities.getEntities().size(),
          "async entities should be merged before publishing sync");
      assertEquals("2",
          secondPublishedEntities.getEntities().get(0).getId(),
          "Order of Async Events Needs to be FIFO");
      assertEquals("3",
          secondPublishedEntities.getEntities().get(1).getId(),
          "Order of Async Events Needs to be FIFO");
    } else {
      TimelineEntities secondAsyncPublish = client.getPublishedEntities(1);
      assertEquals("2",
          secondAsyncPublish.getEntities().get(0).getId(),
          "Order of Async Events Needs to be FIFO");
      TimelineEntities thirdAsyncPublish = client.getPublishedEntities(2);
      assertEquals("3",
          thirdAsyncPublish.getEntities().get(0).getId(),
          "Order of Async Events Needs to be FIFO");
    }

    // test the last entity published is sync put
    TimelineEntities thirdPublishedEntities =
        client.getPublishedEntities(lastPublishIndex);
    assertEquals(1,
        thirdPublishedEntities.getEntities().size(),
        "sync entities had to be published at the last");
    assertEquals("4",
        thirdPublishedEntities.getEntities().get(0).getId(),
        "Expected last sync Event is not proper");
  }

  @Test
  void testExceptionCalls() throws Exception {
    client.setThrowYarnException(true);
    try {
      client.putEntitiesAsync(generateEntity("1"));
    } catch (YarnException e) {
      fail("Async calls are not expected to throw exception");
    }

    try {
      client.putEntities(generateEntity("2"));
      fail("Sync calls are expected to throw exception");
    } catch (YarnException e) {
      assertEquals("ActualException", e.getCause().getMessage(),
          "Same exception needs to be thrown");
    }
  }

  @Test
  void testConfigurableNumberOfMerges() throws Exception {
    client.setSleepBeforeReturn(true);
    try {
      // At max 3 entities need to be merged
      client.putEntitiesAsync(generateEntity("1"));
      client.putEntitiesAsync(generateEntity("2"));
      client.putEntitiesAsync(generateEntity("3"));
      client.putEntitiesAsync(generateEntity("4"));
      client.putEntities(generateEntity("5"));
      client.putEntitiesAsync(generateEntity("6"));
      client.putEntitiesAsync(generateEntity("7"));
      client.putEntitiesAsync(generateEntity("8"));
      client.putEntitiesAsync(generateEntity("9"));
      client.putEntitiesAsync(generateEntity("10"));
    } catch (YarnException e) {
      fail("No exception expected");
    }
    // not having the same logic here as it doesn't depend on how many times
    // events are published.
    Thread.sleep(2 * TIME_TO_SLEEP);
    printReceivedEntities();
    for (TimelineEntities publishedEntities : client.publishedEntities) {
      assertTrue(
          publishedEntities.getEntities().size() <= 3,
          "Number of entities should not be greater than 3 for each publish,"
              + " but was " + publishedEntities.getEntities().size());
    }
  }

  @Test
  void testSetTimelineToken() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    assertEquals(0, ugi.getTokens().size());
    assertNull(client.currentTimelineToken,
        "Timeline token in v2 client should not be set");

    Token token = Token.newInstance(
        new byte[0], "kind", new byte[0], "service");
    client.setTimelineCollectorInfo(CollectorInfo.newInstance(null, token));
    assertNull(client.currentTimelineToken,
        "Timeline token in v2 client should not be set as token kind " + "is unexpected.");
    assertEquals(0, ugi.getTokens().size());

    token = Token.newInstance(new byte[0], TimelineDelegationTokenIdentifier.
        KIND_NAME.toString(), new byte[0], null);
    client.setTimelineCollectorInfo(CollectorInfo.newInstance(null, token));
    assertNull(client.currentTimelineToken,
        "Timeline token in v2 client should not be set as serice is " + "not set.");
    assertEquals(0, ugi.getTokens().size());

    TimelineDelegationTokenIdentifier ident =
        new TimelineDelegationTokenIdentifier(new Text(ugi.getUserName()),
            new Text("renewer"), null);
    ident.setSequenceNumber(1);
    token = Token.newInstance(ident.getBytes(),
        TimelineDelegationTokenIdentifier.KIND_NAME.toString(), new byte[0],
        "localhost:1234");
    client.setTimelineCollectorInfo(CollectorInfo.newInstance(null, token));
    assertEquals(1, ugi.getTokens().size());
    assertNotNull(client.currentTimelineToken,
        "Timeline token should be set in v2 client.");
    assertEquals(token, client.currentTimelineToken);

    ident.setSequenceNumber(20);
    Token newToken = Token.newInstance(ident.getBytes(),
        TimelineDelegationTokenIdentifier.KIND_NAME.toString(), new byte[0],
        "localhost:1234");
    client.setTimelineCollectorInfo(CollectorInfo.newInstance(null, newToken));
    assertEquals(1, ugi.getTokens().size());
    assertNotEquals(token, client.currentTimelineToken);
    assertEquals(newToken, client.currentTimelineToken);
  }

  @Test
  void testAfterStop() throws Exception {
    client.setSleepBeforeReturn(true);
    try {
      // At max 3 entities need to be merged
      client.putEntities(generateEntity("1"));
      for (int i = 2; i < 20; i++) {
        client.putEntitiesAsync(generateEntity("" + i));
      }
      client.stop();
      try {
        client.putEntitiesAsync(generateEntity("50"));
        fail("Exception expected");
      } catch (YarnException e) {
        // expected
      }
    } catch (YarnException e) {
      fail("No exception expected");
    }
    // not having the same logic here as it doesn't depend on how many times
    // events are published.
    for (int i = 0; i < 5; i++) {
      TimelineEntities publishedEntities =
          client.publishedEntities.get(client.publishedEntities.size() - 1);
      TimelineEntity timelineEntity = publishedEntities.getEntities()
          .get(publishedEntities.getEntities().size() - 1);
      if (!timelineEntity.getId().equals("19")) {
        Thread.sleep(2 * TIME_TO_SLEEP);
      }
    }
    printReceivedEntities();
    TimelineEntities publishedEntities =
        client.publishedEntities.get(client.publishedEntities.size() - 1);
    TimelineEntity timelineEntity = publishedEntities.getEntities()
        .get(publishedEntities.getEntities().size() - 1);
    assertEquals("19", timelineEntity.getId(), "");
  }

  private void printReceivedEntities() {
    for (int i = 0; i < client.getNumOfTimelineEntitiesPublished(); i++) {
      TimelineEntities publishedEntities = client.getPublishedEntities(i);
      StringBuilder entitiesPerPublish = new StringBuilder();
      for (TimelineEntity entity : publishedEntities.getEntities()) {
        entitiesPerPublish.append(entity.getId());
        entitiesPerPublish.append(",");
      }
      LOG.info("Entities Published @ index " + i + " : "
          + entitiesPerPublish.toString());
    }
  }

  private static TimelineEntity generateEntity(String id) {
    TimelineEntity entity = new TimelineEntity();
    entity.setId(id);
    entity.setType("testEntity");
    entity.setCreatedTime(System.currentTimeMillis());
    return entity;
  }

  @AfterEach
  public void tearDown() {
    if (client != null) {
      client.stop();
    }
  }
}

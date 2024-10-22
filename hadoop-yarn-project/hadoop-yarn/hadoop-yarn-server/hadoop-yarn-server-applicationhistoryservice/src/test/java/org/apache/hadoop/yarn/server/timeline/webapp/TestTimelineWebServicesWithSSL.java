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

package org.apache.hadoop.yarn.server.timeline.webapp;

import java.io.File;
import java.net.URI;
import java.util.EnumSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.jodah.failsafe.RetryPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.impl.DirectTimelineWriter;
import org.apache.hadoop.yarn.client.api.impl.TimelineClientImpl;
import org.apache.hadoop.yarn.client.api.impl.TimelineWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimelineWebServicesWithSSL {

  private static final String BASEDIR =
      System.getProperty("test.build.dir", "target/test-dir") + "/"
          + TestTimelineWebServicesWithSSL.class.getSimpleName();

  private static String keystoresDir;
  private static String sslConfDir;
  private static ApplicationHistoryServer timelineServer;
  private static TimelineStore store;
  private static Configuration conf;

  @BeforeAll
  public static void setupServer() throws Exception {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STORE,
        MemoryTimelineStore.class, TimelineStore.class);
    conf.set(YarnConfiguration.YARN_HTTP_POLICY_KEY, "HTTPS_ONLY");
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.0f);

    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir =
        KeyStoreTestUtil.getClasspathDir(TestTimelineWebServicesWithSSL.class);

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    conf.addResource("ssl-server.xml");
    conf.addResource("ssl-client.xml");

    timelineServer = new ApplicationHistoryServer();
    timelineServer.init(conf);
    timelineServer.start();
    store = timelineServer.getTimelineStore();
  }

  @AfterAll
  public static void tearDownServer() throws Exception {
    if (timelineServer != null) {
      timelineServer.stop();
    }
  }

  @Test
  void testPutEntities() throws Exception {
    TestTimelineClient client = new TestTimelineClient();
    try {
      client.init(conf);
      client.start();
      TimelineEntity expectedEntity = new TimelineEntity();
      expectedEntity.setEntityType("test entity type");
      expectedEntity.setEntityId("test entity id");
      expectedEntity.setDomainId("test domain id");
      TimelineEvent event = new TimelineEvent();
      event.setEventType("test event type");
      event.setTimestamp(0L);
      expectedEntity.addEvent(event);

      TimelinePutResponse response = client.putEntities(expectedEntity);
      assertEquals(0, response.getErrors().size());
      assertTrue(client.resp.toString().contains("https"));

      TimelineEntity actualEntity = store.getEntity(
          expectedEntity.getEntityId(), expectedEntity.getEntityType(),
          EnumSet.allOf(Field.class));
      assertNotNull(actualEntity);
      assertEquals(
          expectedEntity.getEntityId(), actualEntity.getEntityId());
      assertEquals(
          expectedEntity.getEntityType(), actualEntity.getEntityType());
    } finally {
      client.stop();
      client.close();
    }
  }

  private static class TestTimelineClient extends TimelineClientImpl {

    private Response resp;

    @Override
    protected TimelineWriter createTimelineWriter(Configuration conf,
        UserGroupInformation authUgi, Client client, URI resURI, RetryPolicy<Object> retryPolicy) {
      return new DirectTimelineWriter(authUgi, client, resURI, retryPolicy) {
        @Override
        public Response doPostingObject(Object obj, String path) throws JsonProcessingException {
          resp = super.doPostingObject(obj, path);
          return resp;
        }
      };
    }
  }
}

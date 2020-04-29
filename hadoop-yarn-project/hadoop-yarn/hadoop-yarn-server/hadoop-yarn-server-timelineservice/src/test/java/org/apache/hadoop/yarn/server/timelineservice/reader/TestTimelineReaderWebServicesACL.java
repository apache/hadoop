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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TestFileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

/**
 * Tests ACL check while retrieving entity-types per application.
 */
public class TestTimelineReaderWebServicesACL {

  private static final String ROOT_DIR = new File("target",
      TestTimelineReaderWebServicesACL.class.getSimpleName()).
      getAbsolutePath();

  private int serverPort;
  private TimelineReaderServer server;
  private static final String ADMIN = "yarn";

  @BeforeClass
  public static void setup() throws Exception {
    TestFileSystemTimelineReaderImpl.initializeDataDirectory(ROOT_DIR);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(ROOT_DIR));
  }

  @Before
  public void init() throws Exception {
    try {
      Configuration config = new YarnConfiguration();
      config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      config.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
          "localhost:0");
      config.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
      config.setClass(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
          FileSystemTimelineReaderImpl.class, TimelineReader.class);
      config.set(FileSystemTimelineReaderImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          ROOT_DIR);
      config.setBoolean(YarnConfiguration.FILTER_ENTITY_LIST_BY_USER, true);
      config.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
      config.set(YarnConfiguration.YARN_ADMIN_ACL, ADMIN);
      server = new TimelineReaderServer();
      server.init(config);
      server.start();
      serverPort = server.getWebServerPort();
    } catch (Exception e) {
      Assert.fail("Web server failed to start");
    }
  }

  @After
  public void stop() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  private static ClientResponse verifyHttpResponse(Client client, URI uri,
      Status expectedStatus) {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertNotNull(resp);
    assertEquals(resp.getStatusInfo().getStatusCode(),
        expectedStatus.getStatusCode());
    return resp;
  }

  private static Client createClient() {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    return new Client(new URLConnectionClientHandler(
        new DummyURLConnectionFactory()), cfg);
  }

  private static class DummyURLConnectionFactory
      implements HttpURLConnectionFactory {
    @Override
    public HttpURLConnection getHttpURLConnection(final URL url)
        throws IOException {
      try {
        return (HttpURLConnection)url.openConnection();
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      }
    }
  }

  @Test
  public void testGetEntityTypes() throws Exception {
    Client client = createClient();
    try {
      String unAuthorizedUser ="user2";
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name="+unAuthorizedUser);
      String msg = "User " + unAuthorizedUser
          + " is not allowed to read TimelineService V2 data.";
      ClientResponse resp = verifyHttpResponse(client, uri, Status.FORBIDDEN);
      assertTrue(resp.getEntity(String.class).contains(msg));

      String authorizedUser ="user1";
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name="+authorizedUser);
      verifyHttpResponse(client, uri, Status.OK);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name="+ADMIN);
      verifyHttpResponse(client, uri, Status.OK);

      // Verify with Query Parameter userid
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name="+authorizedUser
          + "&userid="+authorizedUser);
      verifyHttpResponse(client, uri, Status.OK);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name="+authorizedUser
          + "&userid="+unAuthorizedUser);
      verifyHttpResponse(client, uri, Status.FORBIDDEN);
    } finally {
      client.destroy();
    }
  }

}

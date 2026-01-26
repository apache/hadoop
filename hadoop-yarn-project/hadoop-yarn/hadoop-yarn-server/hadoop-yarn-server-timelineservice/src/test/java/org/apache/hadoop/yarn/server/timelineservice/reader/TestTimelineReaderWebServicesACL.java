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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TestFileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

  @BeforeAll
  public static void setup() throws Exception {
    TestFileSystemTimelineReaderImpl.initializeDataDirectory(ROOT_DIR);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(ROOT_DIR));
  }

  @BeforeEach
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
      fail("Web server failed to start");
    }
  }

  @AfterEach
  public void stop() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  private static Response verifyHttpResponse(Client client, URI uri,
      Response.Status expectedStatus) {
    Response resp = client.target(uri).request(MediaType.APPLICATION_JSON).get(Response.class);
    assertNotNull(resp);
    assertEquals(resp.getStatusInfo().getStatusCode(), expectedStatus.getStatusCode());
    return resp;
  }

  private static Client createClient() {
    ClientConfig cfg = new ClientConfig();
    cfg.register(YarnJacksonJaxbJsonProvider.class);
    cfg.connectorProvider(
        new HttpUrlConnectorProvider().connectionFactory(new DummyURLConnectionFactory()));
    return ClientBuilder.newClient(cfg);
  }

  private static class DummyURLConnectionFactory
      implements HttpUrlConnectorProvider.ConnectionFactory {
    @Override
    public HttpURLConnection getConnection(final URL url)
        throws IOException {
      try {
        return (HttpURLConnection)url.openConnection();
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      }
    }
  }

  @Test
  void testGetEntityTypes() throws Exception {
    Client client = createClient();
    try {
      String unAuthorizedUser = "user2";
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name=" + unAuthorizedUser);
      String msg = "User " + unAuthorizedUser
          + " is not allowed to read TimelineService V2 data.";
      Response resp = verifyHttpResponse(client, uri, Response.Status.FORBIDDEN);
      assertTrue(resp.readEntity(String.class).contains(msg));

      String authorizedUser = "user1";
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name=" + authorizedUser);
      verifyHttpResponse(client, uri, Response.Status.OK);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name=" + ADMIN);
      verifyHttpResponse(client, uri, Response.Status.OK);

      // Verify with Query Parameter userid
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name=" + authorizedUser
          + "&userid=" + authorizedUser);
      verifyHttpResponse(client, uri, Response.Status.OK);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entity-types?user.name=" + authorizedUser
          + "&userid=" + unAuthorizedUser);
      verifyHttpResponse(client, uri, Response.Status.FORBIDDEN);
    } finally {
      client.close();
    }
  }

}

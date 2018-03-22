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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.DataGeneratorForTest;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.junit.Assert;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

/**
 * Test Base for TimelineReaderServer HBase tests.
 */
public abstract class AbstractTimelineReaderHBaseTestBase {
  private static int serverPort;
  private static TimelineReaderServer server;
  private static HBaseTestingUtility util;

  public static void setup() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    util.startMiniCluster();
    DataGeneratorForTest.createSchema(util.getConfiguration());
  }

  public static void tearDown() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
    if (util != null) {
      util.shutdownMiniCluster();
    }
  }

  protected static void initialize() throws Exception {
    try {
      Configuration config = util.getConfiguration();
      config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      config.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
          "localhost:0");
      config.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
      config.set(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
          "org.apache.hadoop.yarn.server.timelineservice.storage."
              + "HBaseTimelineReaderImpl");
      config.setInt("hfile.format.version", 3);
      server = new TimelineReaderServer() {
        @Override
        protected void addFilters(Configuration conf) {
          // The parent code uses hadoop-common jar from this version of
          // Hadoop, but the tests are using hadoop-common jar from
          // ${hbase-compatible-hadoop.version}.  This version uses Jetty 9
          // while ${hbase-compatible-hadoop.version} uses Jetty 6, and there
          // are many differences, including classnames and packages.
          // We do nothing here, so that we don't cause a NoSuchMethodError or
          // NoClassDefFoundError.
          // Once ${hbase-compatible-hadoop.version} is changed to Hadoop 3,
          // we should be able to remove this @Override.
        }
      };
      server.init(config);
      server.start();
      serverPort = server.getWebServerPort();
    } catch (Exception e) {
      Assert.fail("Web server failed to start");
    }
  }

  protected Client createClient() {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    return new Client(
        new URLConnectionClientHandler(new DummyURLConnectionFactory()), cfg);
  }

  protected ClientResponse getResponse(Client client, URI uri)
      throws Exception {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    if (resp == null || resp.getStatusInfo()
        .getStatusCode() != ClientResponse.Status.OK.getStatusCode()) {
      String msg = "";
      if (resp != null) {
        msg = String.valueOf(resp.getStatusInfo().getStatusCode());
      }
      throw new IOException(
          "Incorrect response from timeline reader. " + "Status=" + msg);
    }
    return resp;
  }

  protected void verifyHttpResponse(Client client, URI uri, Status status) {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertNotNull(resp);
    assertTrue("Response from server should have been " + status,
        resp.getStatusInfo().getStatusCode() == status.getStatusCode());
    System.out.println("Response is: " + resp.getEntity(String.class));
  }

  protected List<FlowActivityEntity> verifyFlowEntites(Client client, URI uri,
      int noOfEntities) throws Exception {
    ClientResponse resp = getResponse(client, uri);
    List<FlowActivityEntity> entities =
        resp.getEntity(new GenericType<List<FlowActivityEntity>>() {
        });
    assertNotNull(entities);
    assertEquals(noOfEntities, entities.size());
    return entities;
  }

  protected static class DummyURLConnectionFactory
      implements HttpURLConnectionFactory {

    @Override
    public HttpURLConnection getHttpURLConnection(final URL url)
        throws IOException {
      try {
        return (HttpURLConnection) url.openConnection();
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      }
    }
  }

  protected static HBaseTestingUtility getHBaseTestingUtility() {
    return util;
  }

  public static int getServerPort() {
    return serverPort;
  }
}

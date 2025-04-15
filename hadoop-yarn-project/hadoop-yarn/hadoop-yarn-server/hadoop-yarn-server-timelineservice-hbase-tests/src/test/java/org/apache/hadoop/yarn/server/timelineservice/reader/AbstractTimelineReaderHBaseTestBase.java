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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.DataGeneratorForTest;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;

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
    try {
      util.startMiniCluster();
    } catch (Exception e) {
      // TODO catch InaccessibleObjectException directly once Java 8 support is dropped
      if (e.getClass().getSimpleName().equals("InaccessibleObjectException")) {
        assumeTrue(false, "Could not start HBase because of HBASE-29234");
      } else {
        throw e;
      }
    }
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
      fail("Web server failed to start");
    }
  }

  protected Client createClient() {
    final ClientConfig cc = new ClientConfig();
    cc.connectorProvider(getHttpURLConnectionFactory());
    return ClientBuilder.newClient(cc)
        .register(TimelineEntityReader.class)
        .register(TimelineEntitySetReader.class)
        .register(TimelineEntityListReader.class)
        .register(FlowActivityEntityReader.class)
        .register(FlowRunEntityReader.class)
        .register(FlowActivityEntitySetReader.class)
        .register(FlowActivityEntityListReader.class)
        .register(FlowRunEntitySetReader.class);
  }

  protected Response getResponse(Client client, URI uri)
      throws Exception {
    Response resp =
        client.target(uri).request(MediaType.APPLICATION_JSON).get();
    if (resp == null || resp.getStatusInfo()
        .getStatusCode() != HttpURLConnection.HTTP_OK) {
      String msg = "";
      if (resp != null) {
        msg = String.valueOf(resp.getStatusInfo().getStatusCode());
      }
      throw new IOException(
          "Incorrect response from timeline reader. " + "Status=" + msg);
    }
    return resp;
  }

  protected void verifyHttpResponse(Client client, URI uri, Response.Status status) {
    Response resp = client.target(uri).request(MediaType.APPLICATION_JSON).get();
    assertNotNull(resp);
    assertTrue(resp.getStatusInfo().getStatusCode() == status.getStatusCode(),
        "Response from server should have been " + status);
    System.out.println("Response is: " + resp.readEntity(String.class));
  }

  protected List<FlowActivityEntity> verifyFlowEntites(Client client, URI uri,
      int noOfEntities) throws Exception {
    Response resp = getResponse(client, uri);
    List<FlowActivityEntity> entities =
        resp.readEntity(new GenericType<List<FlowActivityEntity>>() {
        });
    assertNotNull(entities);
    assertEquals(noOfEntities, entities.size());
    return entities;
  }

  @VisibleForTesting
  protected HttpUrlConnectorProvider getHttpURLConnectionFactory() {
    return new HttpUrlConnectorProvider().connectionFactory(
        url -> {
          HttpURLConnection conn;
          try {
            conn =  (HttpURLConnection) url.openConnection();
          } catch (Exception e) {
            throw new IOException(e);
          }
          return conn;
        });
  }

  protected static HBaseTestingUtility getHBaseTestingUtility() {
    return util;
  }

  public static int getServerPort() {
    return serverPort;
  }
}

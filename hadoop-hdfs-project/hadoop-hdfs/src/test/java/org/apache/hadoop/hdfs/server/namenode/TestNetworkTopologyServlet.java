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
package org.apache.hadoop.hdfs.server.namenode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.StaticMapping;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestNetworkTopologyServlet {

  @Test
  public void testPrintTopologyTextFormat() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    int dataNodesNum = 0;
    final ArrayList<String> rackList = new ArrayList<String>();
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 2; j++) {
        rackList.add("/rack" + i);
        dataNodesNum++;
      }
    }

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(dataNodesNum)
            .racks(rackList.toArray(new String[rackList.size()]))
            .build();
    cluster.waitActive();

    // get http uri
    String httpUri = cluster.getHttpUri(0);

    // send http request
    URL url = new URL(httpUri + "/topology");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setReadTimeout(20000);
    conn.setConnectTimeout(20000);
    conn.connect();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    StringBuilder sb =
        new StringBuilder("-- Network Topology -- \n");
    sb.append(out);
    sb.append("\n-- Network Topology -- ");
    String topology = sb.toString();

    // assert rack info
    assertTrue(topology.contains("/rack0"));
    assertTrue(topology.contains("/rack1"));
    assertTrue(topology.contains("/rack2"));
    assertTrue(topology.contains("/rack3"));
    assertTrue(topology.contains("/rack4"));

    // assert node number
    assertEquals(topology.split("127.0.0.1").length - 1,
        dataNodesNum);
  }

  @Test
  public void testPrintTopologyJsonFormat() throws IOException {
      StaticMapping.resetMap();
      Configuration conf = new HdfsConfiguration();
      int dataNodesNum = 0;
      final ArrayList<String> rackList = new ArrayList<String>();
      for (int i = 0; i < 5; i++) {
          for (int j = 0; j < 2; j++) {
              rackList.add("/rack" + i);
              dataNodesNum++;
          }
      }

      MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
              .numDataNodes(dataNodesNum)
              .racks(rackList.toArray(new String[rackList.size()]))
              .build();
      cluster.waitActive();

      // get http uri
      String httpUri = cluster.getHttpUri(0);

      // send http request
      URL url = new URL(httpUri + "/topology");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setReadTimeout(20000);
      conn.setConnectTimeout(20000);
      conn.setRequestProperty("Accept", "application/json");
      conn.connect();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
      String topology = out.toString();

      // parse json
      JsonNode racks = new ObjectMapper().readTree(topology);

      // assert rack number
      assertEquals(racks.size(), 5);

      // assert node number
      Iterator<JsonNode> elements = racks.elements();
      int dataNodesCount = 0;
      while(elements.hasNext()){
          JsonNode rack = elements.next();
          Iterator<Map.Entry<String, JsonNode>> fields = rack.fields();
          while (fields.hasNext()) {
              dataNodesCount += fields.next().getValue().size();
          }
      }
      assertEquals(dataNodesCount, dataNodesNum);
  }

  @Test
  public void testPrintTopologyNoDatanodesTextFormat() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(0)
            .build();
    cluster.waitActive();

    // get http uri
    String httpUri = cluster.getHttpUri(0);

    // send http request
    URL url = new URL(httpUri + "/topology");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setReadTimeout(20000);
    conn.setConnectTimeout(20000);
    conn.connect();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
    StringBuilder sb =
            new StringBuilder("-- Network Topology -- \n");
    sb.append(out);
    sb.append("\n-- Network Topology -- ");
    String topology = sb.toString();

    // assert node number
    assertTrue(topology.contains("No DataNodes"));
  }

    @Test
    public void testPrintTopologyNoDatanodesJsonFormat() throws IOException {
        StaticMapping.resetMap();
        Configuration conf = new HdfsConfiguration();
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                .numDataNodes(0)
                .build();
        cluster.waitActive();

        // get http uri
        String httpUri = cluster.getHttpUri(0);

        // send http request
        URL url = new URL(httpUri + "/topology");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(20000);
        conn.setConnectTimeout(20000);
        conn.setRequestProperty("Accept", "application/json");
        conn.connect();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
        StringBuilder sb =
                new StringBuilder("-- Network Topology -- \n");
        sb.append(out);
        sb.append("\n-- Network Topology -- ");
        String topology = sb.toString();

        // assert node number
        assertTrue(topology.contains("No DataNodes"));
    }
}

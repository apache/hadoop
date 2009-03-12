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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Tests that the dfs cluster can be started and stopped and that it appears to
 * work
 */
public class TestMiniDFSCluster extends TestCase {

  private Log log = LogFactory.getLog(getClass());
  private static final int CONNECT_TIMEOUT = 1000;
  private static final int PORT_OPEN_TIMEOUT = 20000;
  private MiniDFSCluster cluster;


  /**
   * Tears down the fixture, for example, close a network connection. This method
   * is called after a test is executed.
   */
  @Override
  protected void tearDown() throws Exception {
    MiniDFSCluster.close(cluster);
    super.tearDown();
  }

  private MiniDFSCluster createCluster() throws IOException {
    MiniDFSCluster miniCluster;
    Configuration conf = new Configuration();
    log.info("Starting the cluster");
    miniCluster = new MiniDFSCluster(conf, 1, true, null);
    log.info("cluster is created");
    log.info(miniCluster);
    return miniCluster;
  }

  public void testClusterStartStop() throws Throwable {
    long start = System.currentTimeMillis();
    cluster = createCluster();
    long live = System.currentTimeMillis();
    log.info("cluster is live -time: "
            + (live - start) / 1000.0 + " millis");
    log.info("terminating cluster");
    cluster.shutdown();
    long terminated = System.currentTimeMillis();
    log.info("cluster is terminated -time: "
            + (terminated - live) / 1000.0 + " millis");
    assertClusterIsNotLive(cluster);
    assertFalse("Datanodes are still up after cluster Termination",
            cluster.isDataNodeUp());
  }

  private void assertClusterIsNotLive(MiniDFSCluster cluster) {
    assertFalse("Cluster still thinks it is live", cluster.isClusterUp());
  }


  /**
   * Check that shutting down twice is OK
   *
   * @throws Throwable if something went wrong
   */
  public void testClusterDoubleShutdown() throws Throwable {
    cluster = createCluster();
    log.info("terminating cluster");
    MiniDFSCluster.close(cluster);
    assertClusterIsNotLive(cluster);
    log.info("terminating cluster again");
    cluster.shutdown();
    log.info("cluster is terminated");
    assertClusterIsNotLive(cluster);
    //and finish by terminating again
    cluster = null;
  }


  /**
   * Check that a cluster is not live until asked
   *
   * @throws Throwable if something went wrong
   */
  public void testClusterGoesLive() throws Throwable {
    cluster = createCluster();
    assertTrue("Cluster thinks it is not live", cluster.isClusterUp());
    assertTrue("No datanodes are up", cluster.isDataNodeUp());
    log.info("terminating cluster");
    MiniDFSCluster.close(cluster);
    cluster = null;
  }

  /**
   * Check that shutting down twice is OK
   *
   * @throws Throwable if something went wrong
   */
  public void testClusterNetworkVisible() throws Throwable {
    cluster = createCluster();
    NameNode namenode = cluster.getNameNode();
    int infoServerPort = namenode.getHttpAddress().getPort();

    int nameNodePort = cluster.getNameNodePort();
    assertPortOpen("Name Node", nameNodePort);
    assertPortOpen("Name Node Info Server", infoServerPort);
    ArrayList<DataNode> datanodes = cluster.getDataNodes();
    ArrayList<Integer> ports = new ArrayList<Integer>(datanodes.size());
    for (DataNode node : datanodes) {
      int port = node.getSelfAddr().getPort();
      ports.add(port);
      assertPortOpen("Data Node " + node, port);
    }
    log.info("terminating cluster");
    MiniDFSCluster.close(cluster);
    assertClusterIsNotLive(cluster);
    assertPortClosed("Name Node", nameNodePort);
    assertPortClosed("Name Node Info Server", infoServerPort);
    assertPortsClosed("Data Node", ports);
  }

  private void assertPortsClosed(String name, ArrayList<Integer> ports)
          throws Throwable {
    for (int port : ports) {
      assertPortClosed(name, port);
    }
  }

  void assertPortOpen(String name, int port) throws Throwable {
    assertPortOpen(name, port, PORT_OPEN_TIMEOUT);
  }
  
  void assertPortOpen(String name, int port, int timeout) throws Throwable {
    Socket socket = null;
    SocketAddress sa = new InetSocketAddress("localhost", port);
    IOException failure = null;
    long endtime = System.currentTimeMillis() + timeout;
    boolean connected = false;
    do {
      try {
        socket = new Socket();
        socket.connect(sa, CONNECT_TIMEOUT);
        connected = true;
      } catch (IOException e) {
        failure = e;
      } finally {
        socket.close();
      }
    } while (!connected && System.currentTimeMillis() < endtime);

    if (failure != null) {
      AssertionFailedError afe =
              new AssertionFailedError("Failed to connect to the service "
                      + name + " on port " + port + " : " + failure);
      afe.initCause(failure);
      throw afe;
    }
  }

  void assertPortClosed(String name, int port) throws Throwable {
    Socket socket = new Socket();
    try {
      SocketAddress sa = new InetSocketAddress("localhost", port);
      socket.connect(sa, CONNECT_TIMEOUT);
      fail("Expected the service " + name
              + " on port " + port + " to be offline, but it is open");
    } catch (IOException expected) {
      log.debug("IOE checking port " + port + ": " + expected,
              expected);
    } finally {
      socket.close();
    }
  }
}

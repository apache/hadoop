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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;

import org.junit.After;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDFSClientSocketSize {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestDFSClientSocketSize.class);
  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }

  private final Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  private Socket socket;

  @Test
  public void testDefaultSendBufferSize() throws IOException {
    socket = createSocket();
    assertEquals("Send buffer size should be the default value.",
        DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_DEFAULT,
        socket.getSendBufferSize());
  }

  @Test
  public void testSpecifiedSendBufferSize() throws IOException {
    final int mySendBufferSize = 64 * 1024;  // 64 KB
    conf.setInt(DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_KEY, mySendBufferSize);
    socket = createSocket();
    assertEquals("Send buffer size should be the customized value.",
        mySendBufferSize, socket.getSendBufferSize());
  }

  @Test
  public void testAutoTuningSendBufferSize() throws IOException {
    conf.setInt(DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_KEY, 0);
    socket = createSocket();
    LOG.info("The auto tuned send buffer size is: {}",
        socket.getSendBufferSize());
    assertTrue("Send buffer size should be non-negative value which is " +
        "determined by system (kernel).", socket.getSendBufferSize() > 0);
  }

  @After
  public void tearDown() throws Exception {
    if (socket != null) {
      LOG.info("Closing the DFSClient socket.");
    }
    if (cluster != null) {
      LOG.info("Shutting down MiniDFSCluster.");
      cluster.shutdown();
    }
  }

  private Socket createSocket() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    LOG.info("MiniDFSCluster started.");
    return DFSOutputStream.createSocketForPipeline(
        new DatanodeInfo(cluster.dataNodes.get(0).datanode.getDatanodeId()),
        1, cluster.getFileSystem().getClient());
  }
}

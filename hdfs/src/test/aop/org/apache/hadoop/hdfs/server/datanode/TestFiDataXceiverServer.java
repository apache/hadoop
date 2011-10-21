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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * This is a test for DataXceiverServer when DataXceiver thread spawning is
 * failed due to OutOfMemoryError. Expected behavior is that DataXceiverServer
 * should not be exited. It should retry again after 30 seconds
 */
public class TestFiDataXceiverServer {

  @Test(timeout = 30000)
  public void testOutOfMemoryErrorInDataXceiverServerRun() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    ServerSocket sock = new ServerSocket() {
      @Override
      public Socket accept() throws IOException {
        return new Socket() {
          @Override
          public InetAddress getInetAddress() {
            return super.getLocalAddress();
          }

          @Override
          public SocketAddress getRemoteSocketAddress() {
            return new InetSocketAddress(8080);
          }

          @Override
          public SocketAddress getLocalSocketAddress() {
            return new InetSocketAddress(0);
          }

          @Override
          public synchronized void close() throws IOException {
            latch.countDown();
            super.close();
          }
        };
      }
    };
    Thread thread = null;
    System.setProperty("fi.enabledOOM", "true");
    DataNode dn = Mockito.mock(DataNode.class);
    try {
      Configuration conf = new Configuration();
      Mockito.doReturn(conf).when(dn).getConf();
      dn.shouldRun = true;
      DataXceiverServer server = new DataXceiverServer(sock, conf, dn);
      thread = new Thread(server);
      thread.start();
      latch.await();
      assertTrue("Not running the thread", thread.isAlive());
    } finally {
      System.setProperty("fi.enabledOOM", "false");
      dn.shouldRun = false;
      if (null != thread)
        thread.interrupt();
      sock.close();
    }
  }
}

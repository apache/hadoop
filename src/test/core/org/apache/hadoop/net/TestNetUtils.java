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
package org.apache.hadoop.net;

import org.junit.Test;
import static org.junit.Assert.*;

import java.net.Socket;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;

public class TestNetUtils {

  /**
   * Test that we can't accidentally connect back to the connecting socket due
   * to a quirk in the TCP spec.
   *
   * This is a regression test for HADOOP-6722.
   */
  @Test
  public void testAvoidLoopbackTcpSockets() throws Exception {
    Configuration conf = new Configuration();

    Socket socket = NetUtils.getDefaultSocketFactory(conf)
      .createSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    System.err.println("local address: " + socket.getLocalAddress());
    System.err.println("local port: " + socket.getLocalPort());
    try {
      NetUtils.connect(socket,
        new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort()),
        20000);
      socket.close();
      fail("Should not have connected");
    } catch (ConnectException ce) {
      System.err.println("Got exception: " + ce);
      assertTrue(ce.getMessage().contains("resulted in a loopback"));
    }
  }
}

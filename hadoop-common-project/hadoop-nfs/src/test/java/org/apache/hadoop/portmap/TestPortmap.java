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

package org.apache.hadoop.portmap;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import org.junit.Assert;

import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class TestPortmap {
  private static Portmap pm = new Portmap();
  private static final int SHORT_TIMEOUT_MILLISECONDS = 10;
  private static final int RETRY_TIMES = 5;
  private int xid;

  @BeforeClass
  public static void setup() {
    pm.start(SHORT_TIMEOUT_MILLISECONDS, new InetSocketAddress("localhost", 0),
        new InetSocketAddress("localhost", 0));
  }

  @AfterClass
  public static void tearDown() {
    pm.shutdown();
  }

  @Test(timeout = 10000)
  public void testIdle() throws InterruptedException, IOException {
    Socket s = new Socket();
    try {
      s.connect(pm.getTcpServerLocalAddress());

      int i = 0;
      while (!s.isConnected() && i < RETRY_TIMES) {
        ++i;
        Thread.sleep(SHORT_TIMEOUT_MILLISECONDS);
      }

      Assert.assertTrue("Failed to connect to the server", s.isConnected()
          && i < RETRY_TIMES);

      int b = s.getInputStream().read();
      Assert.assertTrue("The server failed to disconnect", b == -1);
    } finally {
      s.close();
    }
  }

  @Test(timeout = 10000)
  public void testRegistration() throws IOException, InterruptedException {
    XDR req = new XDR();
    RpcCall.getInstance(++xid, RpcProgramPortmap.PROGRAM,
        RpcProgramPortmap.VERSION,
        RpcProgramPortmap.PMAPPROC_SET,
        new CredentialsNone(), new VerifierNone()).write(req);

    PortmapMapping sent = new PortmapMapping(90000, 1,
        PortmapMapping.TRANSPORT_TCP, 1234);
    sent.serialize(req);

    byte[] reqBuf = req.getBytes();
    DatagramSocket s = new DatagramSocket();
    DatagramPacket p = new DatagramPacket(reqBuf, reqBuf.length,
        pm.getUdpServerLoAddress());
    try {
      s.send(p);
    } finally {
      s.close();
    }

    // Give the server a chance to process the request
    Thread.sleep(100);
    boolean found = false;
    @SuppressWarnings("unchecked")
    Map<String, PortmapMapping> map = (Map<String, PortmapMapping>) Whitebox
        .getInternalState(pm.getHandler(), "map");

    for (PortmapMapping m : map.values()) {
      if (m.getPort() == sent.getPort()
          && PortmapMapping.key(m).equals(PortmapMapping.key(sent))) {
        found = true;
        break;
      }
    }
    Assert.assertTrue("Registration failed", found);
  }
}

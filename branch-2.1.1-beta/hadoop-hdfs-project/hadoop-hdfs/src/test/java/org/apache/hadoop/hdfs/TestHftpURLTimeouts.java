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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.web.URLUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHftpURLTimeouts {
  @BeforeClass
  public static void setup() {
    URLUtils.SOCKET_TIMEOUT = 5;
  }
  
  @Test
  public void testHftpSocketTimeout() throws Exception {
    Configuration conf = new Configuration();
    ServerSocket socket = new ServerSocket(0,1);
    URI uri = new URI("hftp", null,
        InetAddress.getByName(null).getHostAddress(),
        socket.getLocalPort(),
        null, null, null);
    boolean timedout = false;

    HftpFileSystem fs = (HftpFileSystem)FileSystem.get(uri, conf);
    try {
      HttpURLConnection conn = fs.openConnection("/", "");
      timedout = false;
      try {
        // this will consume the only slot in the backlog
        conn.getInputStream();
      } catch (SocketTimeoutException ste) {
        timedout = true;
        assertEquals("Read timed out", ste.getMessage());
      } finally {
        if (conn != null) conn.disconnect();
      }
      assertTrue("read timedout", timedout);
      assertTrue("connect timedout", checkConnectTimeout(fs, false));
    } finally {
      fs.close();
    }
  }

  @Test
  public void testHsftpSocketTimeout() throws Exception {
    Configuration conf = new Configuration();
    ServerSocket socket = new ServerSocket(0,1);
    URI uri = new URI("hsftp", null,
        InetAddress.getByName(null).getHostAddress(),
        socket.getLocalPort(),
        null, null, null);
    boolean timedout = false;

    HsftpFileSystem fs = (HsftpFileSystem)FileSystem.get(uri, conf);
    try {
      HttpURLConnection conn = null;
      timedout = false;
      try {
        // this will consume the only slot in the backlog
        conn = fs.openConnection("/", "");
      } catch (SocketTimeoutException ste) {
        // SSL expects a negotiation, so it will timeout on read, unlike hftp
        timedout = true;
        assertEquals("Read timed out", ste.getMessage());
      } finally {
        if (conn != null) conn.disconnect();
      }
      assertTrue("ssl read connect timedout", timedout);
      assertTrue("connect timedout", checkConnectTimeout(fs, true));
    } finally {
      fs.close();
    }
  }
  
  private boolean checkConnectTimeout(HftpFileSystem fs, boolean ignoreReadTimeout)
      throws IOException {
    boolean timedout = false;
    List<HttpURLConnection> conns = new LinkedList<HttpURLConnection>();
    try {
      // with a listen backlog of 1, should only have to make one connection
      // to trigger a connection timeout.  however... linux doesn't honor the
      // socket's listen backlog so we have to try a bunch of times
      for (int n=32; !timedout && n > 0; n--) {
        try {
          conns.add(fs.openConnection("/", ""));
        } catch (SocketTimeoutException ste) {
          String message = ste.getMessage();
          assertNotNull(message);
          // https will get a read timeout due to SSL negotiation, but
          // a normal http will not, so need to ignore SSL read timeouts
          // until a connect timeout occurs
          if (!(ignoreReadTimeout && message.equals("Read timed out"))) {
            timedout = true;
            assertEquals("connect timed out", message);
          }
        }
      }
    } finally {
      for (HttpURLConnection conn : conns) {
        conn.disconnect();
      }
    }
    return timedout;
  }
}

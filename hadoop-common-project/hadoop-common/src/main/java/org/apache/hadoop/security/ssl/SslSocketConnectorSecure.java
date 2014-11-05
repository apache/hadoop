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

package org.apache.hadoop.security.ssl;

import org.mortbay.jetty.security.SslSocketConnector;

import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;

/**
 * This subclass of the Jetty SslSocketConnector exists solely to control
 * the TLS protocol versions allowed.  This is fallout from the POODLE
 * vulnerability (CVE-2014-3566), which requires that SSLv3 be disabled.
 * Only TLS 1.0 and later protocols are allowed.
 */
public class SslSocketConnectorSecure extends SslSocketConnector {

  public SslSocketConnectorSecure() {
    super();
  }

  /**
   * Create a new ServerSocket that will not accept SSLv3 connections,
   * but will accept TLSv1.x connections.
   */
  protected ServerSocket newServerSocket(String host, int port,int backlog)
          throws IOException {
    SSLServerSocket socket = (SSLServerSocket)
            super.newServerSocket(host, port, backlog);
    ArrayList<String> nonSSLProtocols = new ArrayList<String>();
    for (String p : socket.getEnabledProtocols()) {
      if (!p.contains("SSLv3")) {
        nonSSLProtocols.add(p);
      }
    }
    socket.setEnabledProtocols(nonSSLProtocols.toArray(
            new String[nonSSLProtocols.size()]));
    return socket;
  }
}

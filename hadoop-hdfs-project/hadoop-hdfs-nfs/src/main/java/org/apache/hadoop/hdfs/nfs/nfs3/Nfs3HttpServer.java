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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;

/**
 * Encapsulates the HTTP server started by the NFS3 gateway.
 */
class Nfs3HttpServer {
  private int infoPort;
  private int infoSecurePort;

  private HttpServer2 httpServer;

  private final NfsConfiguration conf;

  Nfs3HttpServer(NfsConfiguration conf) {
    this.conf = conf;
  }

  void start() throws IOException {
    final InetSocketAddress httpAddr = getHttpAddress(conf);

    final String httpsAddrString = conf.get(
        NfsConfigKeys.NFS_HTTPS_ADDRESS_KEY,
        NfsConfigKeys.NFS_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(conf,
        httpAddr, httpsAddr, "nfs3",
        NfsConfigKeys.DFS_NFS_KERBEROS_PRINCIPAL_KEY,
        NfsConfigKeys.DFS_NFS_KEYTAB_FILE_KEY);

    this.httpServer = builder.build();
    this.httpServer.start();
    
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      infoPort = httpServer.getConnectorAddress(connIdx++).getPort();
    }

    if (policy.isHttpsEnabled()) {
      infoSecurePort = httpServer.getConnectorAddress(connIdx).getPort();
    }
  }

  void stop() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public int getPort() {
    return this.infoPort;
  }

  public int getSecurePort() {
    return this.infoSecurePort;
  }

  /**
   * Return the URI that locates the HTTP server.
   */
  public URI getServerURI() {
    // getHttpClientScheme() only returns https for HTTPS_ONLY policy. This
    // matches the behavior that the first connector is a HTTPS connector only
    // for HTTPS_ONLY policy.
    InetSocketAddress addr = httpServer.getConnectorAddress(0);
    return URI.create(DFSUtil.getHttpClientScheme(conf) + "://"
        + NetUtils.getHostPortString(addr));
  }

  public InetSocketAddress getHttpAddress(Configuration conf) {
    String addr = conf.get(NfsConfigKeys.NFS_HTTP_ADDRESS_KEY,
        NfsConfigKeys.NFS_HTTP_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr, NfsConfigKeys.NFS_HTTP_PORT_DEFAULT,
        NfsConfigKeys.NFS_HTTP_ADDRESS_KEY);
  }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.UserGroupInformation;
import org.mortbay.jetty.Connector;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class to start a datanode in a secure cluster, first obtaining 
 * privileged resources before main startup and handing them to the datanode.
 */
public class SecureDataNodeStarter implements Daemon {
  /**
   * Stash necessary resources needed for datanode operation in a secure env.
   */
  public static class SecureResources {
    private final ServerSocket streamingSocket;
    private final Connector listener;
    public SecureResources(ServerSocket streamingSocket,
        Connector listener) {

      this.streamingSocket = streamingSocket;
      this.listener = listener;
    }

    public ServerSocket getStreamingSocket() { return streamingSocket; }

    public Connector getListener() { return listener; }
  }
  
  private String [] args;
  private SecureResources resources;

  @Override
  public void init(DaemonContext context) throws Exception {
    System.err.println("Initializing secure datanode resources");
    // Create a new HdfsConfiguration object to ensure that the configuration in
    // hdfs-site.xml is picked up.
    Configuration conf = new HdfsConfiguration();
    
    // Stash command-line arguments for regular datanode
    args = context.getArguments();
    resources = getSecureResources(conf);
  }

  @Override
  public void start() throws Exception {
    System.err.println("Starting regular datanode initialization");
    DataNode.secureMain(args, resources);
  }

  @Override public void destroy() {}
  @Override public void stop() throws Exception { /* Nothing to do */ }

  /**
   * Acquire privileged resources (i.e., the privileged ports) for the data
   * node. The privileged resources consist of the port of the RPC server and
   * the port of HTTP (not HTTPS) server.
   */
  @VisibleForTesting
  public static SecureResources getSecureResources(Configuration conf)
      throws Exception {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    boolean isSecure = UserGroupInformation.isSecurityEnabled();

    // Obtain secure port for data streaming to datanode
    InetSocketAddress streamingAddr  = DataNode.getStreamingAddr(conf);
    int socketWriteTimeout = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        HdfsServerConstants.WRITE_TIMEOUT);

    ServerSocket ss = (socketWriteTimeout > 0) ? 
        ServerSocketChannel.open().socket() : new ServerSocket();
    ss.bind(streamingAddr, 0);

    // Check that we got the port we need
    if (ss.getLocalPort() != streamingAddr.getPort()) {
      throw new RuntimeException(
          "Unable to bind on specified streaming port in secure "
              + "context. Needed " + streamingAddr.getPort() + ", got "
              + ss.getLocalPort());
    }

    if (ss.getLocalPort() > 1023 && isSecure) {
      throw new RuntimeException(
        "Cannot start secure datanode with unprivileged RPC ports");
    }

    System.err.println("Opened streaming server at " + streamingAddr);

    // Bind a port for the web server. The code intends to bind HTTP server to
    // privileged port only, as the client can authenticate the server using
    // certificates if they are communicating through SSL.
    Connector listener = null;
    if (policy.isHttpEnabled()) {
      listener = HttpServer2.createDefaultChannelConnector();
      InetSocketAddress infoSocAddr = DataNode.getInfoAddr(conf);
      listener.setHost(infoSocAddr.getHostName());
      listener.setPort(infoSocAddr.getPort());
      // Open listener here in order to bind to port as root
      listener.open();
      if (listener.getPort() != infoSocAddr.getPort()) {
        throw new RuntimeException("Unable to bind on specified info port in secure " +
            "context. Needed " + streamingAddr.getPort() + ", got " + ss.getLocalPort());
      }
      System.err.println("Successfully obtained privileged resources (streaming port = "
          + ss + " ) (http listener port = " + listener.getConnection() +")");

      if (listener.getPort() > 1023 && isSecure) {
        throw new RuntimeException(
            "Cannot start secure datanode with unprivileged HTTP ports");
      }
      System.err.println("Opened info server at " + infoSocAddr);
    }

    return new SecureResources(ss, listener);
  }

}

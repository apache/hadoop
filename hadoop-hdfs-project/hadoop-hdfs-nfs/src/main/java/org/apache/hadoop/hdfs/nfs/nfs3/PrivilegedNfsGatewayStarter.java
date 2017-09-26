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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;

/**
 * This class is used to allow the initial registration of the NFS gateway with
 * the system portmap daemon to come from a privileged (&lt; 1024) port. This is
 * necessary on certain operating systems to work around this bug in rpcbind:
 * 
 * Red Hat: https://bugzilla.redhat.com/show_bug.cgi?id=731542
 * SLES: https://bugzilla.novell.com/show_bug.cgi?id=823364
 * Debian: https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=594880
 */
public class PrivilegedNfsGatewayStarter implements Daemon {
  static final Log LOG = LogFactory.getLog(PrivilegedNfsGatewayStarter.class);
  private String[] args = null;
  private DatagramSocket registrationSocket = null;
  private Nfs3 nfs3Server = null;

  @Override
  public void init(DaemonContext context) throws Exception {
    System.err.println("Initializing privileged NFS client socket...");
    NfsConfiguration conf = new NfsConfiguration();
    int clientPort = conf.getInt(NfsConfigKeys.DFS_NFS_REGISTRATION_PORT_KEY,
        NfsConfigKeys.DFS_NFS_REGISTRATION_PORT_DEFAULT);
    if (clientPort < 1 || clientPort > 1023) {
      throw new RuntimeException("Must start privileged NFS server with '" +
          NfsConfigKeys.DFS_NFS_REGISTRATION_PORT_KEY + "' configured to a " +
          "privileged port.");
    }

    try {
      InetSocketAddress socketAddress =
                new InetSocketAddress("localhost", clientPort);
      registrationSocket = new DatagramSocket(null);
      registrationSocket.setReuseAddress(true);
      registrationSocket.bind(socketAddress);
    } catch (SocketException e) {
      LOG.error("Init failed for port=" + clientPort, e);
      throw e;
    }
    args = context.getArguments();
  }

  @Override
  public void start() throws Exception {
    nfs3Server = Nfs3.startService(args, registrationSocket);
  }

  @Override
  public void stop() throws Exception {
    if (nfs3Server != null) {
      nfs3Server.stop();
    }
  }

  @Override
  public void destroy() {
    if (registrationSocket != null && !registrationSocket.isClosed()) {
      registrationSocket.close();
    }
  }

}

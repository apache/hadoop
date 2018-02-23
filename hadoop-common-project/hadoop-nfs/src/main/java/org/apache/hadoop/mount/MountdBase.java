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
package org.apache.hadoop.mount;

import java.io.IOException;

import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.SimpleTcpServer;
import org.apache.hadoop.oncrpc.SimpleUdpServer;
import org.apache.hadoop.portmap.PortmapMapping;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Main class for starting mountd daemon. This daemon implements the NFS
 * mount protocol. When receiving a MOUNT request from an NFS client, it checks
 * the request against the list of currently exported file systems. If the
 * client is permitted to mount the file system, rpc.mountd obtains a file
 * handle for requested directory and returns it to the client.
 */
abstract public class MountdBase {
  public static final Logger LOG = LoggerFactory.getLogger(MountdBase.class);
  private final RpcProgram rpcProgram;
  private int udpBoundPort; // Will set after server starts
  private int tcpBoundPort; // Will set after server starts

  public RpcProgram getRpcProgram() {
    return rpcProgram;
  }

  /**
   * Constructor
   * @param program  rpc server which handles mount request
   * @throws IOException fail to construct MountdBase
   */
  public MountdBase(RpcProgram program) throws IOException {
    rpcProgram = program;
  }

  /* Start UDP server */
  private void startUDPServer() {
    SimpleUdpServer udpServer = new SimpleUdpServer(rpcProgram.getPort(),
        rpcProgram, 1);
    rpcProgram.startDaemons();
    try {
      udpServer.run();
    } catch (Throwable e) {
      LOG.error("Failed to start the UDP server.", e);
      if (udpServer.getBoundPort() > 0) {
        rpcProgram.unregister(PortmapMapping.TRANSPORT_UDP,
            udpServer.getBoundPort());
      }
      udpServer.shutdown();
      terminate(1, e);
    }
    udpBoundPort = udpServer.getBoundPort();
  }

  /* Start TCP server */
  private void startTCPServer() {
    SimpleTcpServer tcpServer = new SimpleTcpServer(rpcProgram.getPort(),
        rpcProgram, 1);
    rpcProgram.startDaemons();
    try {
      tcpServer.run();
    } catch (Throwable e) {
      LOG.error("Failed to start the TCP server.", e);
      if (tcpServer.getBoundPort() > 0) {
        rpcProgram.unregister(PortmapMapping.TRANSPORT_TCP,
            tcpServer.getBoundPort());
      }
      tcpServer.shutdown();
      terminate(1, e);
    }
    tcpBoundPort = tcpServer.getBoundPort();
  }

  public void start(boolean register) {
    startUDPServer();
    startTCPServer();
    if (register) {
      ShutdownHookManager.get().addShutdownHook(new Unregister(),
          SHUTDOWN_HOOK_PRIORITY);
      try {
        rpcProgram.register(PortmapMapping.TRANSPORT_UDP, udpBoundPort);
        rpcProgram.register(PortmapMapping.TRANSPORT_TCP, tcpBoundPort);
      } catch (Throwable e) {
        LOG.error("Failed to register the MOUNT service.", e);
        terminate(1, e);
      }
    }
  }

  public void stop() {
    if (udpBoundPort > 0) {
      rpcProgram.unregister(PortmapMapping.TRANSPORT_UDP, udpBoundPort);
      udpBoundPort = 0;
    }
    if (tcpBoundPort > 0) {
      rpcProgram.unregister(PortmapMapping.TRANSPORT_TCP, tcpBoundPort);
      tcpBoundPort = 0;
    }
  }

  /**
   * Priority of the mountd shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 10;

  private class Unregister implements Runnable {
    @Override
    public synchronized void run() {
      stop();
    }
  }

}

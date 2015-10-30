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
package org.apache.hadoop.nfs.nfs3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.SimpleTcpServer;
import org.apache.hadoop.portmap.PortmapMapping;
import org.apache.hadoop.util.ShutdownHookManager;

import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Nfs server. Supports NFS v3 using {@link RpcProgram}.
 * Only TCP server is supported and UDP is not supported.
 */
public abstract class Nfs3Base {
  public static final Log LOG = LogFactory.getLog(Nfs3Base.class);
  private final RpcProgram rpcProgram;
  private int nfsBoundPort; // Will set after server starts

  public RpcProgram getRpcProgram() {
    return rpcProgram;
  }

  protected Nfs3Base(RpcProgram rpcProgram, Configuration conf) {
    this.rpcProgram = rpcProgram;
    LOG.info("NFS server port set to: " + rpcProgram.getPort());
  }

  public void start(boolean register) {
    startTCPServer(); // Start TCP server

    if (register) {
      ShutdownHookManager.get().addShutdownHook(new NfsShutdownHook(),
          SHUTDOWN_HOOK_PRIORITY);
      try {
        rpcProgram.register(PortmapMapping.TRANSPORT_TCP, nfsBoundPort);
      } catch (Throwable e) {
        LOG.fatal("Failed to register the NFSv3 service.", e);
        terminate(1, e);
      }
    }
  }

  private void startTCPServer() {
    SimpleTcpServer tcpServer = new SimpleTcpServer(rpcProgram.getPort(),
        rpcProgram, 0);
    rpcProgram.startDaemons();
    try {
      tcpServer.run();
    } catch (Throwable e) {
      LOG.fatal("Failed to start the TCP server.", e);
      if (tcpServer.getBoundPort() > 0) {
        rpcProgram.unregister(PortmapMapping.TRANSPORT_TCP,
            tcpServer.getBoundPort());
      }
      tcpServer.shutdown();
      terminate(1, e);
    }
    nfsBoundPort = tcpServer.getBoundPort();
  }

  /**
   * Priority of the nfsd shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 10;

  private class NfsShutdownHook implements Runnable {
    @Override
    public synchronized void run() {
      rpcProgram.unregister(PortmapMapping.TRANSPORT_TCP, nfsBoundPort);
      rpcProgram.stopDaemons();
    }
  }
}

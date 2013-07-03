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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.SimpleTcpServer;
import org.apache.hadoop.oncrpc.SimpleUdpServer;
import org.apache.hadoop.util.StringUtils;

/**
 * Portmap service for binding RPC protocols. See RFC 1833 for details.
 */
public class Portmap {
  public static final Log LOG = LogFactory.getLog(Portmap.class);

  private static void startUDPServer(RpcProgramPortmap rpcProgram) {
    rpcProgram.register(PortmapMapping.TRANSPORT_UDP);
    SimpleUdpServer udpServer = new SimpleUdpServer(RpcProgram.RPCB_PORT,
        rpcProgram, 1);
    udpServer.run();
  }

  private static void startTCPServer(final RpcProgramPortmap rpcProgram) {
    rpcProgram.register(PortmapMapping.TRANSPORT_TCP);
    SimpleTcpServer tcpServer = new SimpleTcpServer(RpcProgram.RPCB_PORT,
        rpcProgram, 1);
    tcpServer.run();
  }

  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(Portmap.class, args, LOG);
    RpcProgramPortmap program = new RpcProgramPortmap();
    try {
      startUDPServer(program);
      startTCPServer(program);
    } catch (Throwable e) {
      LOG.fatal("Start server failure");
      System.exit(-1);
    }
  }
}

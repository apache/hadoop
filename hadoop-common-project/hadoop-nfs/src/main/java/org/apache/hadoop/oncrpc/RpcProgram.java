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
package org.apache.hadoop.oncrpc;

import java.io.IOException;
import java.net.DatagramSocket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.security.Verifier;
import org.apache.hadoop.portmap.PortmapMapping;
import org.apache.hadoop.portmap.PortmapRequest;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * Class for writing RPC server programs based on RFC 1050. Extend this class
 * and implement {@link #handleInternal} to handle the requests received.
 */
public abstract class RpcProgram extends SimpleChannelUpstreamHandler {
  private static final Log LOG = LogFactory.getLog(RpcProgram.class);
  public static final int RPCB_PORT = 111;
  private final String program;
  private final String host;
  private int port; // Ephemeral port is chosen later
  private final int progNumber;
  private final int lowProgVersion;
  private final int highProgVersion;
  
  /**
   * If not null, this will be used as the socket to use to connect to the
   * system portmap daemon when registering this RPC server program.
   */
  private final DatagramSocket registrationSocket;
  
  /**
   * Constructor
   * 
   * @param program program name
   * @param host host where the Rpc server program is started
   * @param port port where the Rpc server program is listening to
   * @param progNumber program number as defined in RFC 1050
   * @param lowProgVersion lowest version of the specification supported
   * @param highProgVersion highest version of the specification supported
   */
  protected RpcProgram(String program, String host, int port, int progNumber,
      int lowProgVersion, int highProgVersion,
      DatagramSocket registrationSocket) {
    this.program = program;
    this.host = host;
    this.port = port;
    this.progNumber = progNumber;
    this.lowProgVersion = lowProgVersion;
    this.highProgVersion = highProgVersion;
    this.registrationSocket = registrationSocket;
  }

  /**
   * Register this program with the local portmapper.
   */
  public void register(int transport, int boundPort) {
    if (boundPort != port) {
      LOG.info("The bound port is " + boundPort
          + ", different with configured port " + port);
      port = boundPort;
    }
    // Register all the program versions with portmapper for a given transport
    for (int vers = lowProgVersion; vers <= highProgVersion; vers++) {
      PortmapMapping mapEntry = new PortmapMapping(progNumber, vers, transport,
          port);
      register(mapEntry, true);
    }
  }
  
  /**
   * Unregister this program with the local portmapper.
   */
  public void unregister(int transport, int boundPort) {
    if (boundPort != port) {
      LOG.info("The bound port is " + boundPort
          + ", different with configured port " + port);
      port = boundPort;
    }
    // Unregister all the program versions with portmapper for a given transport
    for (int vers = lowProgVersion; vers <= highProgVersion; vers++) {
      PortmapMapping mapEntry = new PortmapMapping(progNumber, vers, transport,
          port);
      register(mapEntry, false);
    }
  }
  
  /**
   * Register the program with Portmap or Rpcbind
   */
  protected void register(PortmapMapping mapEntry, boolean set) {
    XDR mappingRequest = PortmapRequest.create(mapEntry, set);
    SimpleUdpClient registrationClient = new SimpleUdpClient(host, RPCB_PORT,
        mappingRequest, registrationSocket);
    try {
      registrationClient.run();
    } catch (IOException e) {
      String request = set ? "Registration" : "Unregistration";
      LOG.error(request + " failure with " + host + ":" + port
          + ", portmap entry: " + mapEntry, e);
      throw new RuntimeException(request + " failure", e);
    }
  }

  // Start extra daemons
  public void startDaemons() {}
  
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception {
    RpcInfo info = (RpcInfo) e.getMessage();
    RpcCall call = (RpcCall) info.header();
    if (LOG.isTraceEnabled()) {
      LOG.trace(program + " procedure #" + call.getProcedure());
    }
    
    if (this.progNumber != call.getProgram()) {
      LOG.warn("Invalid RPC call program " + call.getProgram());
      RpcAcceptedReply reply = RpcAcceptedReply.getInstance(call.getXid(),
          AcceptState.PROG_UNAVAIL, Verifier.VERIFIER_NONE);

      XDR out = new XDR();
      reply.write(out);
      ChannelBuffer b = ChannelBuffers.wrappedBuffer(out.asReadOnlyWrap()
          .buffer());
      RpcResponse rsp = new RpcResponse(b, info.remoteAddress());
      RpcUtil.sendRpcResponse(ctx, rsp);
      return;
    }

    int ver = call.getVersion();
    if (ver < lowProgVersion || ver > highProgVersion) {
      LOG.warn("Invalid RPC call version " + ver);
      RpcAcceptedReply reply = RpcAcceptedReply.getInstance(call.getXid(),
          AcceptState.PROG_MISMATCH, Verifier.VERIFIER_NONE);

      XDR out = new XDR();
      reply.write(out);
      out.writeInt(lowProgVersion);
      out.writeInt(highProgVersion);
      ChannelBuffer b = ChannelBuffers.wrappedBuffer(out.asReadOnlyWrap()
          .buffer());
      RpcResponse rsp = new RpcResponse(b, info.remoteAddress());
      RpcUtil.sendRpcResponse(ctx, rsp);
      return;
    }
    
    handleInternal(ctx, info);
  }

  protected abstract void handleInternal(ChannelHandlerContext ctx, RpcInfo info);
  
  @Override
  public String toString() {
    return "Rpc program: " + program + " at " + host + ":" + port;
  }
  
  protected abstract boolean isIdempotent(RpcCall call);
  
  public int getPort() {
    return port;
  }
}

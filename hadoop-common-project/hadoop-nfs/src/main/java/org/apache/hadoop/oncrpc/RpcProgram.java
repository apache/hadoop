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
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.RpcCallCache.CacheEntry;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.hadoop.portmap.PortmapMapping;
import org.apache.hadoop.portmap.PortmapRequest;
import org.jboss.netty.channel.Channel;

/**
 * Class for writing RPC server programs based on RFC 1050. Extend this class
 * and implement {@link #handleInternal} to handle the requests received.
 */
public abstract class RpcProgram {
  private static final Log LOG = LogFactory.getLog(RpcProgram.class);
  public static final int RPCB_PORT = 111;
  private final String program;
  private final String host;
  private final int port;
  private final int progNumber;
  private final int lowProgVersion;
  private final int highProgVersion;
  private final RpcCallCache rpcCallCache;
  
  /**
   * Constructor
   * 
   * @param program program name
   * @param host host where the Rpc server program is started
   * @param port port where the Rpc server program is listening to
   * @param progNumber program number as defined in RFC 1050
   * @param lowProgVersion lowest version of the specification supported
   * @param highProgVersion highest version of the specification supported
   * @param cacheSize size of cache to handle duplciate requests. Size <= 0
   *          indicates no cache.
   */
  protected RpcProgram(String program, String host, int port, int progNumber,
      int lowProgVersion, int highProgVersion, int cacheSize) {
    this.program = program;
    this.host = host;
    this.port = port;
    this.progNumber = progNumber;
    this.lowProgVersion = lowProgVersion;
    this.highProgVersion = highProgVersion;
    this.rpcCallCache = cacheSize > 0 ? new RpcCallCache(program, cacheSize)
        : null;
  }

  /**
   * Register this program with the local portmapper.
   */
  public void register(int transport) {
    // Register all the program versions with portmapper for a given transport
    for (int vers = lowProgVersion; vers <= highProgVersion; vers++) {
      register(vers, transport);
    }
  }
  
  /**
   * Register this program with the local portmapper.
   */
  private void register(int progVersion, int transport) {
    PortmapMapping mapEntry = new PortmapMapping(progNumber, progVersion,
        transport, port);
    register(mapEntry);
  }
  
  /**
   * Register the program with Portmap or Rpcbind
   */
  protected void register(PortmapMapping mapEntry) {
    XDR mappingRequest = PortmapRequest.create(mapEntry);
    SimpleUdpClient registrationClient = new SimpleUdpClient(host, RPCB_PORT,
        mappingRequest);
    try {
      registrationClient.run();
    } catch (IOException e) {
      LOG.error("Registration failure with " + host + ":" + port
          + ", portmap entry: " + mapEntry);
      throw new RuntimeException("Registration failure");
    }
  }

  /**
   * Handle an RPC request.
   * @param rpcCall RPC call that is received
   * @param in xdr with cursor at reading the remaining bytes of a method call
   * @param out xdr output corresponding to Rpc reply
   * @param client making the Rpc request
   * @param channel connection over which Rpc request is received
   * @return response xdr response
   */
  protected abstract XDR handleInternal(RpcCall rpcCall, XDR in, XDR out,
      InetAddress client, Channel channel);
  
  public XDR handle(XDR xdr, InetAddress client, Channel channel) {
    XDR out = new XDR();
    RpcCall rpcCall = RpcCall.read(xdr);
    if (LOG.isDebugEnabled()) {
      LOG.debug(program + " procedure #" + rpcCall.getProcedure());
    }
    
    if (!checkProgram(rpcCall.getProgram())) {
      return programMismatch(out, rpcCall);
    }

    if (!checkProgramVersion(rpcCall.getVersion())) {
      return programVersionMismatch(out, rpcCall);
    }
    
    // Check for duplicate requests in the cache for non-idempotent requests
    boolean idempotent = rpcCallCache != null && !isIdempotent(rpcCall);
    if (idempotent) {
      CacheEntry entry = rpcCallCache.checkOrAddToCache(client, rpcCall.getXid());
      if (entry != null) { // in ache 
        if (entry.isCompleted()) {
          LOG.info("Sending the cached reply to retransmitted request "
              + rpcCall.getXid());
          return entry.getResponse();
        } else { // else request is in progress
          LOG.info("Retransmitted request, transaction still in progress "
              + rpcCall.getXid());
          // TODO: ignore the request?
        }
      }
    }
    
    XDR response = handleInternal(rpcCall, xdr, out, client, channel);
    if (response.size() == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No sync response, expect an async response for request XID="
            + rpcCall.getXid());
      }
    }
    
    // Add the request to the cache
    if (idempotent) {
      rpcCallCache.callCompleted(client, rpcCall.getXid(), response);
    }
    return response;
  }
  
  private XDR programMismatch(XDR out, RpcCall call) {
    LOG.warn("Invalid RPC call program " + call.getProgram());
    RpcAcceptedReply reply = RpcAcceptedReply.getInstance(call.getXid(),
        AcceptState.PROG_UNAVAIL, new VerifierNone());
    reply.write(out);
    return out;
  }
  
  private XDR programVersionMismatch(XDR out, RpcCall call) {
    LOG.warn("Invalid RPC call version " + call.getVersion());
    RpcAcceptedReply reply = RpcAcceptedReply.getInstance(call.getXid(),
        AcceptState.PROG_MISMATCH, new VerifierNone());
    reply.write(out);
    out.writeInt(lowProgVersion);
    out.writeInt(highProgVersion);
    return out;
  }
  
  private boolean checkProgram(int progNumber) {
    return this.progNumber == progNumber;
  }
  
  /** Return true if a the program version in rpcCall is supported */
  private boolean checkProgramVersion(int programVersion) {
    return programVersion >= lowProgVersion
        && programVersion <= highProgVersion;
  }
  
  @Override
  public String toString() {
    return "Rpc program: " + program + " at " + host + ":" + port;
  }
  
  protected abstract boolean isIdempotent(RpcCall call);
  
  public int getPort() {
    return port;
  }
}

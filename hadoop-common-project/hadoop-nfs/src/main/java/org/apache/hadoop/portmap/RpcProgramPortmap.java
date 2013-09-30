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

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcInfo;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.RpcResponse;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;

/**
 * An rpcbind request handler.
 */
public class RpcProgramPortmap extends RpcProgram implements PortmapInterface {
  public static final int PROGRAM = 100000;
  public static final int VERSION = 2;
  
  private static final Log LOG = LogFactory.getLog(RpcProgramPortmap.class);

  /** Map synchronized usis monitor lock of this instance */
  private final HashMap<String, PortmapMapping> map;

  public RpcProgramPortmap() {
    super("portmap", "localhost", RPCB_PORT, PROGRAM, VERSION, VERSION);
    map = new HashMap<String, PortmapMapping>(256);
  }

  /** Dump all the register RPC services */
  private synchronized void dumpRpcServices() {
    Set<Entry<String, PortmapMapping>> entrySet = map.entrySet();
    for (Entry<String, PortmapMapping> entry : entrySet) {
      LOG.info("Service: " + entry.getKey() + " portmapping: "
          + entry.getValue());
    }
  }
  
  @Override
  public XDR nullOp(int xid, XDR in, XDR out) {
    return PortmapResponse.voidReply(out, xid);
  }

  @Override
  public XDR set(int xid, XDR in, XDR out) {
    PortmapMapping mapping = PortmapRequest.mapping(in);
    String key = PortmapMapping.key(mapping);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Portmap set key=" + key);
    }

    PortmapMapping value = null;
    synchronized(this) {
      map.put(key, mapping);
      dumpRpcServices();
      value = map.get(key);
    }  
    return PortmapResponse.intReply(out, xid, value.getPort());
  }

  @Override
  public synchronized XDR unset(int xid, XDR in, XDR out) {
    PortmapMapping mapping = PortmapRequest.mapping(in);
    synchronized(this) {
      map.remove(PortmapMapping.key(mapping));
    }
    return PortmapResponse.booleanReply(out, xid, true);
  }

  @Override
  public synchronized XDR getport(int xid, XDR in, XDR out) {
    PortmapMapping mapping = PortmapRequest.mapping(in);
    String key = PortmapMapping.key(mapping);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Portmap GETPORT key=" + key + " " + mapping);
    }
    PortmapMapping value = null;
    synchronized(this) {
      value = map.get(key);
    }
    int res = 0;
    if (value != null) {
      res = value.getPort();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found mapping for key: " + key + " port:" + res);
      }
    } else {
      LOG.warn("Warning, no mapping for key: " + key);
    }
    return PortmapResponse.intReply(out, xid, res);
  }

  @Override
  public synchronized XDR dump(int xid, XDR in, XDR out) {
    PortmapMapping[] pmapList = null;
    synchronized(this) {
      pmapList = new PortmapMapping[map.values().size()];
      map.values().toArray(pmapList);
    }
    return PortmapResponse.pmapList(out, xid, pmapList);
  }

  @Override
  public void register(PortmapMapping mapping) {
    String key = PortmapMapping.key(mapping);
    synchronized(this) {
      map.put(key, mapping);
    }
  }

  @Override
  public void handleInternal(ChannelHandlerContext ctx, RpcInfo info) {
    RpcCall rpcCall = (RpcCall) info.header();
    final Procedure portmapProc = Procedure.fromValue(rpcCall.getProcedure());
    int xid = rpcCall.getXid();
    byte[] data = new byte[info.data().readableBytes()];
    info.data().readBytes(data);
    XDR in = new XDR(data);
    XDR out = new XDR();

    if (portmapProc == Procedure.PMAPPROC_NULL) {
      out = nullOp(xid, in, out);
    } else if (portmapProc == Procedure.PMAPPROC_SET) {
      out = set(xid, in, out);
    } else if (portmapProc == Procedure.PMAPPROC_UNSET) {
      out = unset(xid, in, out);
    } else if (portmapProc == Procedure.PMAPPROC_DUMP) {
      out = dump(xid, in, out);
    } else if (portmapProc == Procedure.PMAPPROC_GETPORT) {
      out = getport(xid, in, out);
    } else if (portmapProc == Procedure.PMAPPROC_GETVERSADDR) {
      out = getport(xid, in, out);
    } else {
      LOG.info("PortmapHandler unknown rpc procedure=" + portmapProc);
      RpcAcceptedReply reply = RpcAcceptedReply.getInstance(xid,
          RpcAcceptedReply.AcceptState.PROC_UNAVAIL, new VerifierNone());
      reply.write(out);
    }

    ChannelBuffer buf = ChannelBuffers.wrappedBuffer(out.asReadOnlyWrap().buffer());
    RpcResponse rsp = new RpcResponse(buf, info.remoteAddress());
    RpcUtil.sendRpcResponse(ctx, rsp);
  }
  
  @Override
  protected boolean isIdempotent(RpcCall call) {
    return false;
  }
}

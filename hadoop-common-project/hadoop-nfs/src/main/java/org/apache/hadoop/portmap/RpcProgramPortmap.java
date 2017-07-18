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

import java.util.concurrent.ConcurrentHashMap;

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
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelUpstreamHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RpcProgramPortmap extends IdleStateAwareChannelUpstreamHandler {
  static final int PROGRAM = 100000;
  static final int VERSION = 2;

  static final int PMAPPROC_NULL = 0;
  static final int PMAPPROC_SET = 1;
  static final int PMAPPROC_UNSET = 2;
  static final int PMAPPROC_GETPORT = 3;
  static final int PMAPPROC_DUMP = 4;
  static final int PMAPPROC_GETVERSADDR = 9;

  private static final Logger LOG =
      LoggerFactory.getLogger(RpcProgramPortmap.class);

  private final ConcurrentHashMap<String, PortmapMapping> map = new ConcurrentHashMap<String, PortmapMapping>();

  /** ChannelGroup that remembers all active channels for gracefully shutdown. */
  private final ChannelGroup allChannels;

  RpcProgramPortmap(ChannelGroup allChannels) {
    this.allChannels = allChannels;
    PortmapMapping m = new PortmapMapping(PROGRAM, VERSION,
        PortmapMapping.TRANSPORT_TCP, RpcProgram.RPCB_PORT);
    PortmapMapping m1 = new PortmapMapping(PROGRAM, VERSION,
        PortmapMapping.TRANSPORT_UDP, RpcProgram.RPCB_PORT);
    map.put(PortmapMapping.key(m), m);
    map.put(PortmapMapping.key(m1), m1);
  }

  /**
   * This procedure does no work. By convention, procedure zero of any protocol
   * takes no parameters and returns no results.
   */
  private XDR nullOp(int xid, XDR in, XDR out) {
    return PortmapResponse.voidReply(out, xid);
  }

  /**
   * When a program first becomes available on a machine, it registers itself
   * with the port mapper program on the same machine. The program passes its
   * program number "prog", version number "vers", transport protocol number
   * "prot", and the port "port" on which it awaits service request. The
   * procedure returns a boolean reply whose value is "TRUE" if the procedure
   * successfully established the mapping and "FALSE" otherwise. The procedure
   * refuses to establish a mapping if one already exists for the tuple
   * "(prog, vers, prot)".
   */
  private XDR set(int xid, XDR in, XDR out) {
    PortmapMapping mapping = PortmapRequest.mapping(in);
    String key = PortmapMapping.key(mapping);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Portmap set key=" + key);
    }

    map.put(key, mapping);
    return PortmapResponse.intReply(out, xid, mapping.getPort());
  }

  /**
   * When a program becomes unavailable, it should unregister itself with the
   * port mapper program on the same machine. The parameters and results have
   * meanings identical to those of "PMAPPROC_SET". The protocol and port number
   * fields of the argument are ignored.
   */
  private XDR unset(int xid, XDR in, XDR out) {
    PortmapMapping mapping = PortmapRequest.mapping(in);
    String key = PortmapMapping.key(mapping);

    if (LOG.isDebugEnabled())
      LOG.debug("Portmap remove key=" + key);

    map.remove(key);
    return PortmapResponse.booleanReply(out, xid, true);
  }

  /**
   * Given a program number "prog", version number "vers", and transport
   * protocol number "prot", this procedure returns the port number on which the
   * program is awaiting call requests. A port value of zeros means the program
   * has not been registered. The "port" field of the argument is ignored.
   */
  private XDR getport(int xid, XDR in, XDR out) {
    PortmapMapping mapping = PortmapRequest.mapping(in);
    String key = PortmapMapping.key(mapping);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Portmap GETPORT key=" + key + " " + mapping);
    }
    PortmapMapping value = map.get(key);
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

  /**
   * This procedure enumerates all entries in the port mapper's database. The
   * procedure takes no parameters and returns a list of program, version,
   * protocol, and port values.
   */
  private XDR dump(int xid, XDR in, XDR out) {
    PortmapMapping[] pmapList = map.values().toArray(new PortmapMapping[0]);
    return PortmapResponse.pmapList(out, xid, pmapList);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
      throws Exception {

    RpcInfo info = (RpcInfo) e.getMessage();
    RpcCall rpcCall = (RpcCall) info.header();
    final int portmapProc = rpcCall.getProcedure();
    int xid = rpcCall.getXid();
    XDR in = new XDR(info.data().toByteBuffer().asReadOnlyBuffer(),
        XDR.State.READING);
    XDR out = new XDR();

    if (portmapProc == PMAPPROC_NULL) {
      out = nullOp(xid, in, out);
    } else if (portmapProc == PMAPPROC_SET) {
      out = set(xid, in, out);
    } else if (portmapProc == PMAPPROC_UNSET) {
      out = unset(xid, in, out);
    } else if (portmapProc == PMAPPROC_DUMP) {
      out = dump(xid, in, out);
    } else if (portmapProc == PMAPPROC_GETPORT) {
      out = getport(xid, in, out);
    } else if (portmapProc == PMAPPROC_GETVERSADDR) {
      out = getport(xid, in, out);
    } else {
      LOG.info("PortmapHandler unknown rpc procedure=" + portmapProc);
      RpcAcceptedReply reply = RpcAcceptedReply.getInstance(xid,
          RpcAcceptedReply.AcceptState.PROC_UNAVAIL, new VerifierNone());
      reply.write(out);
    }

    ChannelBuffer buf = ChannelBuffers.wrappedBuffer(out.asReadOnlyWrap()
        .buffer());
    RpcResponse rsp = new RpcResponse(buf, info.remoteAddress());
    RpcUtil.sendRpcResponse(ctx, rsp);
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
      throws Exception {
    allChannels.add(e.getChannel());
  }

  @Override
  public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e)
      throws Exception {
    if (e.getState() == IdleState.ALL_IDLE) {
      e.getChannel().close();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    LOG.warn("Encountered ", e.getCause());
    e.getChannel().close();
  }
}

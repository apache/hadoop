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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

/**
 * A simple client that registers an RPC program with portmap.
 */
public class RegistrationClient extends SimpleTcpClient {
  public static final Log LOG = LogFactory.getLog(RegistrationClient.class);

  public RegistrationClient(String host, int port, XDR request) {
    super(host, port, request);
  }

  /**
   * Handler to handle response from the server.
   */
  static class RegistrationClientHandler extends SimpleTcpClientHandler {
    public RegistrationClientHandler(XDR request) {
      super(request);
    }

    private boolean validMessageLength(int len) {
      // 28 bytes is the minimal success response size (portmapV2)
      if (len < 28) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Portmap mapping registration failed,"
              + " the response size is less than 28 bytes:" + len);
        }
        return false;
      }
      return true;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      ChannelBuffer buf = (ChannelBuffer) e.getMessage(); // Read reply
      if (!validMessageLength(buf.readableBytes())) {
        e.getChannel().close();
        return;
      }

      // handling fragment header for TCP, 4 bytes.
      byte[] fragmentHeader = Arrays.copyOfRange(buf.array(), 0, 4);
      int fragmentSize = XDR.fragmentSize(fragmentHeader);
      boolean isLast = XDR.isLastFragment(fragmentHeader);
      assert (fragmentSize == 28 && isLast == true);

      XDR xdr = new XDR();
      xdr.writeFixedOpaque(Arrays.copyOfRange(buf.array(), 4,
          buf.readableBytes()));

      RpcReply reply = RpcReply.read(xdr);
      if (reply.getState() == RpcReply.ReplyState.MSG_ACCEPTED) {
        RpcAcceptedReply acceptedReply = (RpcAcceptedReply) reply;
        handle(acceptedReply, xdr);
      } else {
        RpcDeniedReply deniedReply = (RpcDeniedReply) reply;
        handle(deniedReply);
      }
      e.getChannel().close(); // shutdown now that request is complete
    }

    private void handle(RpcDeniedReply deniedReply) {
      LOG.warn("Portmap mapping registration request was denied , " + 
          deniedReply);
    }

    private void handle(RpcAcceptedReply acceptedReply, XDR xdr) {
      AcceptState acceptState = acceptedReply.getAcceptState();
      assert (acceptState == AcceptState.SUCCESS);
      boolean answer = xdr.readBoolean();
      if (answer != true) {
        LOG.warn("Portmap mapping registration failed, accept state:"
            + acceptState);
      }
      LOG.info("Portmap mapping registration succeeded");
    }
  }
}

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

import java.net.SocketAddress;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

/**
 * RpcInfo records all contextual information of an RPC message. It contains
 * the RPC header, the parameters, and the information of the remote peer.
 */
public final class RpcInfo {
  private final RpcMessage header;
  private final ChannelBuffer data;
  private final Channel channel;
  private final SocketAddress remoteAddress;

  public RpcInfo(RpcMessage header, ChannelBuffer data,
      ChannelHandlerContext channelContext, Channel channel,
      SocketAddress remoteAddress) {
    this.header = header;
    this.data = data;
    this.channel = channel;
    this.remoteAddress = remoteAddress;
  }

  public RpcMessage header() {
    return header;
  }

  public ChannelBuffer data() {
    return data;
  }

  public Channel channel() {
    return channel;
  }

  public SocketAddress remoteAddress() {
    return remoteAddress;
  }
}

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

import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultAddressedEnvelope;

/**
 * RpcResponse encapsulates a response to a RPC request. It contains the data
 * that is going to cross the wire, as well as the information of the remote
 * peer.
 */
public class RpcResponse extends
    DefaultAddressedEnvelope<ByteBuf, SocketAddress> {
  public RpcResponse(ByteBuf message, SocketAddress recipient) {
    super(message, recipient, null);
  }

  public RpcResponse(ByteBuf message, SocketAddress recipient,
      SocketAddress sender) {
    super(message, recipient, sender);
  }

  public ByteBuf data() {
    return this.content();
  }

  public SocketAddress remoteAddress() {
    return this.recipient();
  }
}

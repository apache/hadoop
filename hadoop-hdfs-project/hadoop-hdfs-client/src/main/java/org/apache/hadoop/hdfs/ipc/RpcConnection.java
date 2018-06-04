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
package org.apache.hadoop.hdfs.ipc;

import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;

import com.google.protobuf.CodedOutputStream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.ipc.BufferCallBeforeInitHandler.BufferCallEvent;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.util.ProtoUtil;

/**
 * The connection to remote server.
 */
@InterfaceAudience.Private
class RpcConnection {

  final RpcClient rpcClient;

  final ConnectionId remoteId;

  private final AuthProtocol authProtocol;

  private Channel channel;

  public RpcConnection(RpcClient rpcClient, ConnectionId remoteId,
      AuthProtocol authProtocol) {
    this.rpcClient = rpcClient;
    this.remoteId = remoteId;
    this.authProtocol = authProtocol;
  }

  private void writeConnectionHeader(Channel ch) {
    ByteBuf header = ch.alloc().buffer(7);
    header.writeBytes(RpcConstants.HEADER.duplicate());
    header.writeByte(RpcConstants.CURRENT_VERSION);
    header.writeByte(0); // service class
    header.writeByte(authProtocol.callId);
    ch.writeAndFlush(header);
  }

  private void writeConnectionContext(Channel ch) throws IOException {
    RpcRequestHeaderProto connectionContextHeader =
        ProtoUtil.makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
            OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
            RpcConstants.INVALID_RETRY_COUNT, rpcClient.getClientId());
    int headerSize = connectionContextHeader.getSerializedSize();
    IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
        remoteId.getProtocolName(), remoteId.getTicket(), AuthMethod.SIMPLE);
    int messageSize = message.getSerializedSize();

    int totalSize =
        CodedOutputStream.computeRawVarint32Size(headerSize) + headerSize +
            CodedOutputStream.computeRawVarint32Size(messageSize) + messageSize;
    ByteBuf buf = ch.alloc().buffer(totalSize + 4);
    buf.writeInt(totalSize);
    ByteBufOutputStream out = new ByteBufOutputStream(buf);
    connectionContextHeader.writeDelimitedTo(out);
    message.writeDelimitedTo(out);
    ch.writeAndFlush(buf);
  }

  private void established(Channel ch) throws IOException {
    ChannelPipeline p = ch.pipeline();
    String addBeforeHandler =
        p.context(BufferCallBeforeInitHandler.class).name();
    p.addBefore(addBeforeHandler, "frameDecoder",
        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
    p.addBefore(addBeforeHandler, "rpcHandler", new RpcDuplexHandler(this));
    p.fireUserEventTriggered(BufferCallEvent.success());
  }

  private Channel connect() {
    if (channel != null) {
      return channel;
    }
    channel = new Bootstrap().group(rpcClient.getGroup())
        .channel(rpcClient.getChannelClass())
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .handler(new BufferCallBeforeInitHandler())
        .remoteAddress(remoteId.getAddress()).connect()
        .addListener(new ChannelFutureListener() {

          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            Channel ch = future.channel();
            if (!future.isSuccess()) {
              failInit(ch, IPCUtil.toIOE(future.cause()));
              return;
            }
            writeConnectionHeader(ch);
            writeConnectionContext(ch);
            established(ch);
          }
        }).channel();
    return channel;
  }

  private synchronized void failInit(Channel ch, IOException e) {
    // fail all pending calls
    ch.pipeline().fireUserEventTriggered(BufferCallEvent.fail(e));
    shutdown0();
  }

  private void shutdown0() {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }

  public synchronized void shutdown() {
    shutdown0();
  }

  public synchronized void sendRequest(Call call) {
    Channel channel = connect();
    channel.eventLoop().execute(() -> channel.writeAndFlush(call));
  }
}
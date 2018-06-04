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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.util.ProtoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

@InterfaceAudience.Private
class RpcDuplexHandler extends ChannelDuplexHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(RpcDuplexHandler.class);

  private final RpcConnection conn;

  private final Map<Integer, Call> id2Call = new HashMap<>();

  public RpcDuplexHandler(RpcConnection conn) {
    this.conn = conn;
  }

  private void writeRequest(ChannelHandlerContext ctx, Call call,
      ChannelPromise promise) throws IOException {
    id2Call.put(call.getId(), call);

    RpcRequestHeaderProto rpcHeader = ProtoUtil.makeRpcRequestHeader(
        RpcKind.RPC_PROTOCOL_BUFFER, OperationProto.RPC_FINAL_PACKET,
        call.getId(), 0, conn.rpcClient.getClientId());
    int rpcHeaderSize = rpcHeader.getSerializedSize();
    RequestHeaderProto requestHeader =
        RequestHeaderProto.newBuilder().setMethodName(call.getMethodName())
            .setDeclaringClassProtocolName(call.getProtocolName())
            .setClientProtocolVersion(call.getProtocolVersion()).build();
    int requestHeaderSize = requestHeader.getSerializedSize();
    int totalSize = CodedOutputStream.computeRawVarint32Size(rpcHeaderSize) +
        rpcHeaderSize +
        CodedOutputStream.computeRawVarint32Size(requestHeaderSize) +
        requestHeaderSize;
    Message param = call.getParam();
    if (param != null) {
      int paramSize = param.getSerializedSize();
      totalSize +=
          CodedOutputStream.computeRawVarint32Size(paramSize) + paramSize;
    }
    ByteBufOutputStream out =
        new ByteBufOutputStream(ctx.alloc().buffer(totalSize + 4));
    out.writeInt(totalSize);
    rpcHeader.writeDelimitedTo(out);
    requestHeader.writeDelimitedTo(out);
    if (param != null) {
      param.writeDelimitedTo(out);
    }
    ctx.write(out.buffer(), promise);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) throws Exception {
    if (msg instanceof Call) {
      writeRequest(ctx, (Call) msg, promise);
    } else {
      ctx.write(msg, promise);
    }
  }

  private void readResponse(ChannelHandlerContext ctx, ByteBuf buf)
      throws Exception {
    ByteBufInputStream in = new ByteBufInputStream(buf);
    RpcResponseHeaderProto header =
        RpcResponseHeaderProto.parseDelimitedFrom(in);
    int id = header.getCallId();
    RpcStatusProto status = header.getStatus();
    if (status != RpcStatusProto.SUCCESS) {
      String exceptionClassName =
          header.hasExceptionClassName() ? header.getExceptionClassName()
              : "ServerDidNotSetExceptionClassName";
      String errorMsg = header.hasErrorMsg() ? header.getErrorMsg()
          : "ServerDidNotSetErrorMsg";
      RpcErrorCodeProto errCode =
          (header.hasErrorDetail() ? header.getErrorDetail() : null);
      if (errCode == null) {
        LOG.warn("Detailed error code not set by server on rpc error");
      }
      RemoteException re =
          new RemoteException(exceptionClassName, errorMsg, errCode);
      if (status == RpcStatusProto.ERROR) {
        Call call = id2Call.remove(id);
        call.setException(re);
      } else if (status == RpcStatusProto.FATAL) {
        exceptionCaught(ctx, re);
      }
      return;
    }
    Call call = id2Call.remove(id);
    call.setResponse(call.getResponseDefaultType().getParserForType()
        .parseDelimitedFrom(in));
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
      throws Exception {
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      try {
        readResponse(ctx, buf);
      } finally {
        buf.release();
      }
    }
  }

  private void cleanupCalls(ChannelHandlerContext ctx, IOException error) {
    for (Call call : id2Call.values()) {
      call.setException(error);
    }
    id2Call.clear();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (!id2Call.isEmpty()) {
      cleanupCalls(ctx, new IOException("Connection closed"));
    }
    conn.shutdown();
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    if (!id2Call.isEmpty()) {
      cleanupCalls(ctx, new IOException("Connection closed"));
    }
    conn.shutdown();
  }
}
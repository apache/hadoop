/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.TestRpcBase.TestRpcService;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.AddRequestProto;

import java.io.Closeable;
import java.io.IOException;

public class TestClientProtocolTranslatorPB implements TestClientProtocol, Closeable {
  final private TestRpcService rpcProxy;

  public TestClientProtocolTranslatorPB(TestRpcService rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void ping() throws IOException {
    EmptyRequestProto req = EmptyRequestProto.newBuilder()
        .build();

    AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.ping(null, req),
        res -> null, Void.class);
  }

  @Override
  public String echo(String echoMessage) throws IOException {
    EchoRequestProto req = EchoRequestProto.newBuilder()
        .setMessage(echoMessage)
        .build();

    return AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.echo(null, req),
        res -> res.getMessage(), String.class);
  }

  @Override
  public void error() throws IOException {
    EmptyRequestProto req = EmptyRequestProto.newBuilder()
        .build();

    AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.error(null, req),
        res -> null, Void.class);
  }

  @Override
  public int add(int num1, int num2) throws IOException {
    AddRequestProto req = AddRequestProto.newBuilder()
        .setParam1(num1)
        .setParam2(num2)
        .build();

    return AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.add(null, req),
        res -> res.getResult(), Integer.class);
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }
}

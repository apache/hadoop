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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link SimpleTcpServer} and {@link SimpleUdpServer} bind to the
 * address specified at construction time.
 */
public class TestSimpleServerBind {

  @ChannelHandler.Sharable
  static class NoopRpcProgram extends RpcProgram {
    NoopRpcProgram(int port) {
      super("noop", "localhost", port, 100001, 1, 1, null, true);
    }

    @Override
    protected void handleInternal(ChannelHandlerContext ctx, RpcInfo info) {
      RpcCall rpcCall = (RpcCall) info.header();
      RpcAcceptedReply reply =
          RpcAcceptedReply.getAcceptInstance(rpcCall.getXid(), new VerifierNone());
      XDR out = new XDR();
      reply.write(out);
      ByteBuf b = Unpooled.wrappedBuffer(out.asReadOnlyWrap().buffer());
      RpcUtil.sendRpcResponse(ctx, new RpcResponse(b, info.remoteAddress()));
    }

    @Override
    protected boolean isIdempotent(RpcCall call) {
      return false;
    }
  }

  @Test
  public void testRpcProgramDefaultBindHostIsNull() {
    NoopRpcProgram program = new NoopRpcProgram(9999);
    // Legacy constructor leaves bindHost null so the server uses new InetSocketAddress(port),
    // which respects the JVM's preferred wildcard (0.0.0.0 or :: for IPv6).
    assertNull(program.getBindHost());
  }

  @Test
  public void testRpcProgramCustomBindHost() {
    // Construct directly with the full 10-arg constructor via a subclass that
    // passes a custom bindHost.
    RpcProgram program = new RpcProgram("test", "localhost", 9999,
        100001, 1, 1, null, true, 500, "127.0.0.1") {
      @Override
      protected void handleInternal(ChannelHandlerContext ctx, RpcInfo info) {}

      @Override
      protected boolean isIdempotent(RpcCall call) { return false; }
    };
    assertEquals("127.0.0.1", program.getBindHost());
  }

  @Test
  public void testTcpServerBindsToSpecifiedHost() throws InterruptedException {
    int port = 0;
    NoopRpcProgram program = new NoopRpcProgram(port);
    SimpleTcpServer server = new SimpleTcpServer(port, "127.0.0.1", program, 1);
    try {
      server.run();
      InetSocketAddress addr = server.getBoundAddress();
      assertNotNull(addr);
      assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
      assertTrue(server.getBoundPort() > 0);
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testTcpServerDefaultBindsToWildcard() throws InterruptedException {
    int port = 0;
    NoopRpcProgram program = new NoopRpcProgram(port);
    // Legacy constructor uses new InetSocketAddress(port) — the JVM's wildcard
    // (0.0.0.0 on IPv4, :: on IPv6-first JVMs).
    SimpleTcpServer server = new SimpleTcpServer(port, program, 1);
    try {
      server.run();
      InetSocketAddress addr = server.getBoundAddress();
      assertNotNull(addr);
      assertTrue(addr.getAddress().isAnyLocalAddress(),
          "Legacy constructor should bind to the JVM wildcard address");
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testUdpServerBindsToSpecifiedHost() throws InterruptedException {
    int port = 0;
    NoopRpcProgram program = new NoopRpcProgram(port);
    SimpleUdpServer server = new SimpleUdpServer(port, "127.0.0.1", program, 1);
    try {
      server.run();
      InetSocketAddress addr = server.getBoundAddress();
      assertNotNull(addr);
      assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
      assertTrue(server.getBoundPort() > 0);
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testUdpServerDefaultBindsToWildcard() throws InterruptedException {
    int port = 0;
    NoopRpcProgram program = new NoopRpcProgram(port);
    // Legacy constructor uses new InetSocketAddress(port) — the JVM's wildcard
    // (0.0.0.0 on IPv4, :: on IPv6-first JVMs).
    SimpleUdpServer server = new SimpleUdpServer(port, program, 1);
    try {
      server.run();
      InetSocketAddress addr = server.getBoundAddress();
      assertNotNull(addr);
      assertTrue(addr.getAddress().isAnyLocalAddress(),
          "Legacy constructor should bind to the JVM wildcard address");
    } finally {
      server.shutdown();
    }
  }
}

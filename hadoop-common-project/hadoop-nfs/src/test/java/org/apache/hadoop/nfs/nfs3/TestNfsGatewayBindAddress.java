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
package org.apache.hadoop.nfs.nfs3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mount.MountdBase;
import org.apache.hadoop.oncrpc.RpcAcceptedReply;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.RpcInfo;
import org.apache.hadoop.oncrpc.RpcProgram;
import org.apache.hadoop.oncrpc.RpcResponse;
import org.apache.hadoop.oncrpc.RpcUtil;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the bind address configured via {@code nfs.server.bind.host}
 * flows through {@link Nfs3Base} and {@link MountdBase} all the way to the
 * actual listening sockets.
 *
 * These tests exercise the wiring inside the base classes — specifically that
 * {@code rpcProgram.getBindHost()} is forwarded to {@link
 * org.apache.hadoop.oncrpc.SimpleTcpServer} and {@link
 * org.apache.hadoop.oncrpc.SimpleUdpServer} — so a regression that removes
 * that call would be caught here even if the config-key unit tests still pass.
 */
public class TestNfsGatewayBindAddress {

  // -----------------------------------------------------------------------
  // Minimal stub infrastructure
  // -----------------------------------------------------------------------

  @ChannelHandler.Sharable
  private static class StubRpcProgram extends RpcProgram {
    StubRpcProgram(int port, String bindHost) {
      super("stub", "localhost", port, 100001, 1, 1, null, true, 500, bindHost);
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

  /** Concrete {@link Nfs3Base} subclass that does nothing beyond what the base class does. */
  private static class StubNfs3 extends Nfs3Base {
    StubNfs3(RpcProgram program) {
      super(program, new Configuration());
    }
  }

  /** Concrete {@link MountdBase} subclass that does nothing beyond what the base class does. */
  private static class StubMountd extends MountdBase {
    StubMountd(RpcProgram program) throws IOException {
      super(program);
    }
  }

  private static int randomHighPort() {
    return 20000 + new Random().nextInt(10000);
  }

  // -----------------------------------------------------------------------
  // Nfs3Base (TCP) tests
  // -----------------------------------------------------------------------

  @Test
  public void testNfs3BaseBindsToConfiguredHost() throws IOException {
    int port = randomHighPort();
    StubRpcProgram program = new StubRpcProgram(port, "127.0.0.1");
    StubNfs3 nfs3 = new StubNfs3(program);
    try {
      nfs3.start(false); // false = skip portmap registration
      InetSocketAddress addr = nfs3.getBoundAddress();
      assertNotNull(addr, "NFS3 TCP server should have a bound address");
      assertEquals("127.0.0.1", addr.getAddress().getHostAddress(),
          "NFS3 TCP server should bind to the address from getBindHost()");
    } finally {
      nfs3.stop();
    }
  }

  @Test
  public void testNfs3BaseDefaultBindsToAllInterfaces() throws IOException {
    int port = randomHighPort();
    StubRpcProgram program = new StubRpcProgram(port, "0.0.0.0");
    StubNfs3 nfs3 = new StubNfs3(program);
    try {
      nfs3.start(false);
      InetSocketAddress addr = nfs3.getBoundAddress();
      assertNotNull(addr);
      assertEquals("0.0.0.0", addr.getAddress().getHostAddress(),
          "NFS3 TCP server should bind to all interfaces by default");
    } finally {
      nfs3.stop();
    }
  }

  // -----------------------------------------------------------------------
  // MountdBase (TCP + UDP) tests
  // -----------------------------------------------------------------------

  @Test
  public void testMountdBaseBindsToConfiguredHost() throws IOException {
    int port = randomHighPort();
    StubRpcProgram program = new StubRpcProgram(port, "127.0.0.1");
    StubMountd mountd = new StubMountd(program);
    try {
      mountd.start(false);
      InetSocketAddress tcpAddr = mountd.getBoundTcpAddress();
      assertNotNull(tcpAddr, "Mountd TCP server should have a bound address");
      assertEquals("127.0.0.1", tcpAddr.getAddress().getHostAddress(),
          "Mountd TCP server should bind to the address from getBindHost()");

      InetSocketAddress udpAddr = mountd.getBoundUdpAddress();
      assertNotNull(udpAddr, "Mountd UDP server should have a bound address");
      assertEquals("127.0.0.1", udpAddr.getAddress().getHostAddress(),
          "Mountd UDP server should bind to the address from getBindHost()");
    } finally {
      mountd.stop();
    }
  }

  @Test
  public void testMountdBaseDefaultBindsToAllInterfaces() throws IOException {
    int port = randomHighPort();
    StubRpcProgram program = new StubRpcProgram(port, "0.0.0.0");
    StubMountd mountd = new StubMountd(program);
    try {
      mountd.start(false);
      InetSocketAddress tcpAddr = mountd.getBoundTcpAddress();
      assertNotNull(tcpAddr);
      assertEquals("0.0.0.0", tcpAddr.getAddress().getHostAddress(),
          "Mountd TCP server should bind to all interfaces by default");

      InetSocketAddress udpAddr = mountd.getBoundUdpAddress();
      assertNotNull(udpAddr);
      assertEquals("0.0.0.0", udpAddr.getAddress().getHostAddress(),
          "Mountd UDP server should bind to all interfaces by default");
    } finally {
      mountd.stop();
    }
  }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.oncrpc.RpcUtil.RpcFrameDecoder;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.apache.log4j.Level;
import org.apache.commons.logging.impl.Log4JLogger;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFrameDecoder {
  
  static {
    ((Log4JLogger) RpcProgram.LOG).getLogger().setLevel(Level.ALL);
  }

  private static int resultSize;

  static void testRequest(XDR request, int serverPort) {
    // Reset resultSize so as to avoid interference from other tests in this class.
    resultSize = 0;
    SimpleTcpClient tcpClient = new SimpleTcpClient("localhost", serverPort, request,
        true);
    tcpClient.run();
  }

  static class TestRpcProgram extends RpcProgram {

    protected TestRpcProgram(String program, String host, int port,
        int progNumber, int lowProgVersion, int highProgVersion,
        boolean allowInsecurePorts) {
      super(program, host, port, progNumber, lowProgVersion, highProgVersion,
          null, allowInsecurePorts);
    }

    @Override
    protected void handleInternal(ChannelHandlerContext ctx, RpcInfo info) {
      // This is just like what's done in RpcProgramMountd#handleInternal and
      // RpcProgramNfs3#handleInternal.
      RpcCall rpcCall = (RpcCall) info.header();
      final int procedure = rpcCall.getProcedure();
      if (procedure != 0) {
        boolean portMonitorSuccess = doPortMonitoring(info.remoteAddress());
        if (!portMonitorSuccess) {
          sendRejectedReply(rpcCall, info.remoteAddress(), ctx);
          return;
        }
      }
      
      resultSize = info.data().readableBytes();
      RpcAcceptedReply reply = RpcAcceptedReply.getAcceptInstance(1234,
          new VerifierNone());
      XDR out = new XDR();
      reply.write(out);
      ChannelBuffer b = ChannelBuffers.wrappedBuffer(out.asReadOnlyWrap().buffer());
      RpcResponse rsp = new RpcResponse(b, info.remoteAddress());
      RpcUtil.sendRpcResponse(ctx, rsp);
    }

    @Override
    protected boolean isIdempotent(RpcCall call) {
      return false;
    }
  }

  @Test
  public void testSingleFrame() {
    RpcFrameDecoder decoder = new RpcFrameDecoder();

    // Test "Length field is not received yet"
    ByteBuffer buffer = ByteBuffer.allocate(1);
    ChannelBuffer buf = new ByteBufferBackedChannelBuffer(buffer);
    ChannelBuffer channelBuffer = (ChannelBuffer) decoder.decode(
        Mockito.mock(ChannelHandlerContext.class), Mockito.mock(Channel.class),
        buf);
    assertTrue(channelBuffer == null);

    // Test all bytes are not received yet
    byte[] fragment = new byte[4 + 9];
    fragment[0] = (byte) (1 << 7); // final fragment
    fragment[1] = 0;
    fragment[2] = 0;
    fragment[3] = (byte) 10; // fragment size = 10 bytes
    assertTrue(XDR.isLastFragment(fragment));
    assertTrue(XDR.fragmentSize(fragment)==10);

    buffer = ByteBuffer.allocate(4 + 9);
    buffer.put(fragment);
    buffer.flip();
    buf = new ByteBufferBackedChannelBuffer(buffer);
    channelBuffer = (ChannelBuffer) decoder.decode(
        Mockito.mock(ChannelHandlerContext.class), Mockito.mock(Channel.class),
        buf);
    assertTrue(channelBuffer == null);
  }
  
  @Test
  public void testMultipleFrames() {
    RpcFrameDecoder decoder = new RpcFrameDecoder();

    // Test multiple frames
    byte[] fragment1 = new byte[4 + 10];
    fragment1[0] = 0; // not final fragment
    fragment1[1] = 0;
    fragment1[2] = 0;
    fragment1[3] = (byte) 10; // fragment size = 10 bytes
    assertFalse(XDR.isLastFragment(fragment1));
    assertTrue(XDR.fragmentSize(fragment1)==10);
    
    // decoder should wait for the final fragment
    ByteBuffer buffer = ByteBuffer.allocate(4 + 10);    
    buffer.put(fragment1);
    buffer.flip();
    ChannelBuffer buf = new ByteBufferBackedChannelBuffer(buffer);
    ChannelBuffer channelBuffer = (ChannelBuffer) decoder.decode(
        Mockito.mock(ChannelHandlerContext.class), Mockito.mock(Channel.class),
        buf);
    assertTrue(channelBuffer == null);

    byte[] fragment2 = new byte[4 + 10];
    fragment2[0] = (byte) (1 << 7); // final fragment
    fragment2[1] = 0;
    fragment2[2] = 0;
    fragment2[3] = (byte) 10; // fragment size = 10 bytes
    assertTrue(XDR.isLastFragment(fragment2));
    assertTrue(XDR.fragmentSize(fragment2)==10);

    buffer = ByteBuffer.allocate(4 + 10);
    buffer.put(fragment2);
    buffer.flip();
    buf = new ByteBufferBackedChannelBuffer(buffer);
    channelBuffer = (ChannelBuffer) decoder.decode(
        Mockito.mock(ChannelHandlerContext.class), Mockito.mock(Channel.class),
        buf);
    assertTrue(channelBuffer != null);
    // Complete frame should have to total size 10+10=20
    assertEquals(20, channelBuffer.readableBytes());
  }

  @Test
  public void testFrames() {
    int serverPort = startRpcServer(true);

    XDR xdrOut = createGetportMount();
    int headerSize = xdrOut.size();
    int bufsize = 2 * 1024 * 1024;
    byte[] buffer = new byte[bufsize];
    xdrOut.writeFixedOpaque(buffer);
    int requestSize = xdrOut.size() - headerSize;

    // Send the request to the server
    testRequest(xdrOut, serverPort);

    // Verify the server got the request with right size
    assertEquals(requestSize, resultSize);
  }
  
  @Test
  public void testUnprivilegedPort() {
    // Don't allow connections from unprivileged ports. Given that this test is
    // presumably not being run by root, this will be the case.
    int serverPort = startRpcServer(false);

    XDR xdrOut = createGetportMount();
    int bufsize = 2 * 1024 * 1024;
    byte[] buffer = new byte[bufsize];
    xdrOut.writeFixedOpaque(buffer);

    // Send the request to the server
    testRequest(xdrOut, serverPort);

    // Verify the server rejected the request.
    assertEquals(0, resultSize);
    
    // Ensure that the NULL procedure does in fact succeed.
    xdrOut = new XDR();
    createPortmapXDRheader(xdrOut, 0);
    int headerSize = xdrOut.size();
    buffer = new byte[bufsize];
    xdrOut.writeFixedOpaque(buffer);
    int requestSize = xdrOut.size() - headerSize;
    
    // Send the request to the server
    testRequest(xdrOut, serverPort);

    // Verify the server did not reject the request.
    assertEquals(requestSize, resultSize);
  }
  
  private static int startRpcServer(boolean allowInsecurePorts) {
    Random rand = new Random();
    int serverPort = 30000 + rand.nextInt(10000);
    int retries = 10;    // A few retries in case initial choice is in use.

    while (true) {
      try {
        RpcProgram program = new TestFrameDecoder.TestRpcProgram("TestRpcProgram",
            "localhost", serverPort, 100000, 1, 2, allowInsecurePorts);
        SimpleTcpServer tcpServer = new SimpleTcpServer(serverPort, program, 1);
        tcpServer.run();
        break;          // Successfully bound a port, break out.
      } catch (ChannelException ce) {
        if (retries-- > 0) {
          serverPort += rand.nextInt(20); // Port in use? Try another.
        } else {
          throw ce;     // Out of retries.
        }
      }
    }
    return serverPort;
  }

  static void createPortmapXDRheader(XDR xdr_out, int procedure) {
    // Make this a method
    RpcCall.getInstance(0, 100000, 2, procedure, new CredentialsNone(),
        new VerifierNone()).write(xdr_out);
  }

  static XDR createGetportMount() {
    XDR xdr_out = new XDR();
    createPortmapXDRheader(xdr_out, 3);
    return xdr_out;
  }
  /*
   * static void testGetport() { XDR xdr_out = new XDR();
   * 
   * createPortmapXDRheader(xdr_out, 3);
   * 
   * xdr_out.writeInt(100003); xdr_out.writeInt(3); xdr_out.writeInt(6);
   * xdr_out.writeInt(0);
   * 
   * XDR request2 = new XDR();
   * 
   * createPortmapXDRheader(xdr_out, 3); request2.writeInt(100003);
   * request2.writeInt(3); request2.writeInt(6); request2.writeInt(0);
   * 
   * testRequest(xdr_out); }
   * 
   * static void testDump() { XDR xdr_out = new XDR();
   * createPortmapXDRheader(xdr_out, 4); testRequest(xdr_out); }
   */
}

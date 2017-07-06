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

package org.apache.hadoop.ipc;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.Server.Call;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * This is intended to be a set of unit tests for the 
 * org.apache.hadoop.ipc.Server class.
 */
public class TestServer {

  @Test
  public void testBind() throws Exception {
    Configuration conf = new Configuration();
    ServerSocket socket = new ServerSocket();
    InetSocketAddress address = new InetSocketAddress("0.0.0.0",0);
    socket.bind(address);
    try {
      int min = socket.getLocalPort();
      int max = min + 100;
      conf.set("TestRange", min+"-"+max);
      

      ServerSocket socket2 = new ServerSocket();
      InetSocketAddress address2 = new InetSocketAddress("0.0.0.0", 0);
      Server.bind(socket2, address2, 10, conf, "TestRange");
      try {
        assertTrue(socket2.isBound());
        assertTrue(socket2.getLocalPort() > min);
        assertTrue(socket2.getLocalPort() <= max);
      } finally {
        socket2.close();
      }
    } finally {
      socket.close();
    }
  }
  
  @Test
  public void testBindSimple() throws Exception {
    ServerSocket socket = new ServerSocket();
    InetSocketAddress address = new InetSocketAddress("0.0.0.0",0);
    Server.bind(socket, address, 10);
    try {
      assertTrue(socket.isBound());
    } finally {
      socket.close();
    }
  }

  @Test
  public void testEmptyConfig() throws Exception {
    Configuration conf = new Configuration();
    conf.set("TestRange", "");


    ServerSocket socket = new ServerSocket();
    InetSocketAddress address = new InetSocketAddress("0.0.0.0", 0);
    try {
      Server.bind(socket, address, 10, conf, "TestRange");
      assertTrue(socket.isBound());
    } finally {
      socket.close();
    }
  }
  
  
  @Test
  public void testBindError() throws Exception {
    Configuration conf = new Configuration();
    ServerSocket socket = new ServerSocket();
    InetSocketAddress address = new InetSocketAddress("0.0.0.0",0);
    socket.bind(address);
    try {
      int min = socket.getLocalPort();
      conf.set("TestRange", min+"-"+min);
      

      ServerSocket socket2 = new ServerSocket();
      InetSocketAddress address2 = new InetSocketAddress("0.0.0.0", 0);
      boolean caught = false;
      try {
        Server.bind(socket2, address2, 10, conf, "TestRange");
      } catch (BindException e) {
        caught = true;
      } finally {
        socket2.close();
      }
      assertTrue("Failed to catch the expected bind exception",caught);
    } finally {
      socket.close();
    }
  }
  
  static class TestException1 extends Exception {
  }

  static class TestException2 extends Exception {
  }

  static class TestException3 extends Exception {
  }

  @Test (timeout=300000)
  public void testLogExceptions() throws Exception {
    final Configuration conf = new Configuration();
    final Call dummyCall = new Call(0, 0, null, null);
    Logger logger = mock(Logger.class);
    Server server = new Server("0.0.0.0", 0, LongWritable.class, 1, conf) {
      @Override
      public Writable call(
          RPC.RpcKind rpcKind, String protocol, Writable param,
          long receiveTime) throws Exception {
        return null;
      }
    };
    server.addSuppressedLoggingExceptions(TestException1.class);
    server.addTerseExceptions(TestException2.class);

    // Nothing should be logged for a suppressed exception.
    server.logException(logger, new TestException1(), dummyCall);
    verifyZeroInteractions(logger);

    // No stack trace should be logged for a terse exception.
    server.logException(logger, new TestException2(), dummyCall);
    verify(logger, times(1)).info(anyObject());

    // Full stack trace should be logged for other exceptions.
    final Throwable te3 = new TestException3();
    server.logException(logger, te3, dummyCall);
    verify(logger, times(1)).info(anyObject(), eq(te3));
  }

  @Test
  public void testExceptionsHandlerTerse() {
    Server.ExceptionsHandler handler = new Server.ExceptionsHandler();
    handler.addTerseLoggingExceptions(IOException.class);
    handler.addTerseLoggingExceptions(RpcServerException.class, IpcException.class);

    assertTrue(handler.isTerseLog(IOException.class));
    assertTrue(handler.isTerseLog(RpcServerException.class));
    assertTrue(handler.isTerseLog(IpcException.class));
    assertFalse(handler.isTerseLog(RpcClientException.class));
  }

  @Test
  public void testExceptionsHandlerSuppressed() {
    Server.ExceptionsHandler handler = new Server.ExceptionsHandler();
    handler.addSuppressedLoggingExceptions(IOException.class);
    handler.addSuppressedLoggingExceptions(RpcServerException.class, IpcException.class);

    assertTrue(handler.isSuppressedLog(IOException.class));
    assertTrue(handler.isSuppressedLog(RpcServerException.class));
    assertTrue(handler.isSuppressedLog(IpcException.class));
    assertFalse(handler.isSuppressedLog(RpcClientException.class));
  }
}

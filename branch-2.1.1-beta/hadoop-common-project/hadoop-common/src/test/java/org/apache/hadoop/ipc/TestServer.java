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

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

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
  
  @Test
  public void testExceptionsHandler() {
    Server.ExceptionsHandler handler = new Server.ExceptionsHandler();
    handler.addTerseExceptions(IOException.class);
    handler.addTerseExceptions(RpcServerException.class, IpcException.class);

    assertTrue(handler.isTerse(IOException.class));
    assertTrue(handler.isTerse(RpcServerException.class));
    assertTrue(handler.isTerse(IpcException.class));
    assertFalse(handler.isTerse(RpcClientException.class));
  }
}

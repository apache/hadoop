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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class RpcConstants {
  private RpcConstants() {
    // Hidden Constructor
  }
  
  public static final int AUTHORIZATION_FAILED_CALL_ID = -1;
  public static final int INVALID_CALL_ID = -2;
  public static final int CONNECTION_CONTEXT_CALL_ID = -3;
  public static final int PING_CALL_ID = -4;
  
  public static final byte[] DUMMY_CLIENT_ID = new byte[0];
  
  
  public static final int INVALID_RETRY_COUNT = -1;
  
 /**
  * The Rpc-connection header is as follows 
  * +----------------------------------+
  * |  "hrpc" 4 bytes                  |      
  * +----------------------------------+
  * |  Version (1 byte)                |
  * +----------------------------------+
  * |  Service Class (1 byte)          |
  * +----------------------------------+
  * |  AuthProtocol (1 byte)           |      
  * +----------------------------------+
  */
  
  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final ByteBuffer HEADER =
      ByteBuffer.wrap("hrpc".getBytes(StandardCharsets.UTF_8));
  public static final int HEADER_LEN_AFTER_HRPC_PART = 3; // 3 bytes that follow
  
  // 1 : Introduce ping and server does not throw away RPCs
  // 3 : Introduce the protocol into the RPC connection header
  // 4 : Introduced SASL security layer
  // 5 : Introduced use of {@link ArrayPrimitiveWritable$Internal}
  //     in ObjectWritable to efficiently transmit arrays of primitives
  // 6 : Made RPC Request header explicit
  // 7 : Changed Ipc Connection Header to use Protocol buffers
  // 8 : SASL server always sends a final response
  // 9 : Changes to protocol for HADOOP-8990
  public static final byte CURRENT_VERSION = 9;
}

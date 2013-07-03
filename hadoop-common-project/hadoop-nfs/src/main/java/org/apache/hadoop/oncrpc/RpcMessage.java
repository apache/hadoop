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

/**
 * Represent an RPC message as defined in RFC 1831.
 */
public abstract class RpcMessage {
  public static final int RPC_CALL = 0;
  public static final int RPC_REPLY = 1;

  protected final int xid;
  protected final int messageType;
  
  RpcMessage(int xid, int messageType) {
    if (messageType != RPC_CALL && messageType != RPC_REPLY) {
      throw new IllegalArgumentException("Invalid message type " + messageType);
    }
    this.xid = xid;
    this.messageType = messageType;
  }
  
  public int getXid() {
    return xid;
  }

  public int getMessageType() {
    return messageType;
  }
  
  protected void validateMessageType(int expected) {
    if (expected != messageType) {
      throw new IllegalArgumentException("Message type is expected to be "
          + expected + " but got " + messageType);
    }
  }
}

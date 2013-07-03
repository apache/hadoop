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
 * Represents an RPC message of type RPC reply as defined in RFC 1831
 */
public abstract class RpcReply extends RpcMessage {
  /** RPC reply_stat as defined in RFC 1831 */
  public enum ReplyState {
    MSG_ACCEPTED(0),
    MSG_DENIED(1);
    
    private final int value;
    ReplyState(int value) {
      this.value = value;
    }
    
    int getValue() {
      return value;
    }
    
    public static ReplyState fromValue(int value) {
      return values()[value];
    }
  }
  
  private final ReplyState state;
  
  RpcReply(int xid, int messageType, ReplyState state) {
    super(xid, messageType);
    this.state = state;
    validateMessageType(RPC_REPLY);
  }

  public static RpcReply read(XDR xdr) {
    int xid = xdr.readInt();
    int messageType = xdr.readInt();
    ReplyState stat = ReplyState.fromValue(xdr.readInt());
    switch (stat) {
    case MSG_ACCEPTED:
      return RpcAcceptedReply.read(xid, messageType, stat, xdr);
    case MSG_DENIED:
      return RpcDeniedReply.read(xid, messageType, stat, xdr);
    }
    return null;
  }

  public ReplyState getState() {
    return state;
  }
}

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

import org.apache.hadoop.oncrpc.security.RpcAuthInfo;
import org.apache.hadoop.oncrpc.security.Verifier;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Represents an RPC message of type RPC reply as defined in RFC 1831
 */
public abstract class RpcReply extends RpcMessage {
  /** RPC reply_stat as defined in RFC 1831 */
  public enum ReplyState {
    // the order of the values below are significant.
    MSG_ACCEPTED,
    MSG_DENIED;
    
    int getValue() {
      return ordinal();
    }
    
    public static ReplyState fromValue(int value) {
      return values()[value];
    }
  }
  
  protected final ReplyState replyState;
  protected final Verifier verifier;
  
  RpcReply(int xid, ReplyState state, Verifier verifier) {
    super(xid, RpcMessage.Type.RPC_REPLY);
    this.replyState = state;
    this.verifier = verifier;
  }
  
  public RpcAuthInfo getVerifier() {
    return verifier;
  }

  public static RpcReply read(XDR xdr) {
    int xid = xdr.readInt();
    final Type messageType = Type.fromValue(xdr.readInt());
    Preconditions.checkState(messageType == RpcMessage.Type.RPC_REPLY);
    
    ReplyState stat = ReplyState.fromValue(xdr.readInt());
    switch (stat) {
    case MSG_ACCEPTED:
      return RpcAcceptedReply.read(xid, stat, xdr);
    case MSG_DENIED:
      return RpcDeniedReply.read(xid, stat, xdr);
    }
    return null;
  }

  public ReplyState getState() {
    return replyState;
  }
}

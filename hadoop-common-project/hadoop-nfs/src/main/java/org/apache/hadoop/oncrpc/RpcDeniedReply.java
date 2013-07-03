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

import org.apache.hadoop.oncrpc.RpcAuthInfo.AuthFlavor;

/** 
 * Represents RPC message MSG_DENIED reply body. See RFC 1831 for details.
 * This response is sent to a request to indicate failure of the request.
 */
public class RpcDeniedReply extends RpcReply {
  public enum RejectState {
    RPC_MISMATCH(0), AUTH_ERROR(1);

    private final int value;

    RejectState(int value) {
      this.value = value;
    }

    int getValue() {
      return value;
    }

    static RejectState fromValue(int value) {
      return values()[value];
    }
  }

  private final RejectState rejectState;

  RpcDeniedReply(int xid, int messageType, ReplyState replyState,
      RejectState rejectState) {
    super(xid, messageType, replyState);
    this.rejectState = rejectState;
  }

  public static RpcDeniedReply read(int xid, int messageType,
      ReplyState replyState, XDR xdr) {
    RejectState rejectState = RejectState.fromValue(xdr.readInt());
    return new RpcDeniedReply(xid, messageType, replyState, rejectState);
  }

  public RejectState getRejectState() {
    return rejectState;
  }
  
  @Override
  public String toString() {
    return new StringBuffer().append("xid:").append(xid)
        .append(",messageType:").append(messageType).append("rejectState:")
        .append(rejectState).toString();
  }
  
  public static XDR voidReply(XDR xdr, int xid, ReplyState msgAccepted,
      RejectState rejectState) {
    xdr.writeInt(xid);
    xdr.writeInt(RpcMessage.RPC_REPLY);
    xdr.writeInt(msgAccepted.getValue());
    xdr.writeInt(AuthFlavor.AUTH_NONE.getValue());
    xdr.writeVariableOpaque(new byte[0]);
    xdr.writeInt(rejectState.getValue());
    return xdr;
  }
}

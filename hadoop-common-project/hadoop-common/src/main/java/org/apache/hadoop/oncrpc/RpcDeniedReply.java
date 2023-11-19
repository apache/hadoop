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

import org.apache.hadoop.oncrpc.security.Verifier;

/** 
 * Represents RPC message MSG_DENIED reply body. See RFC 1831 for details.
 * This response is sent to a request to indicate failure of the request.
 */
public class RpcDeniedReply extends RpcReply {
  public enum RejectState {
    // the order of the values below are significant.
    RPC_MISMATCH,
    AUTH_ERROR;

    int getValue() {
      return ordinal();
    }

    static RejectState fromValue(int value) {
      return values()[value];
    }
  }

  private final RejectState rejectState;

  public RpcDeniedReply(int xid, ReplyState replyState,
      RejectState rejectState, Verifier verifier) {
    super(xid, replyState, verifier);
    this.rejectState = rejectState;
  }

  public static RpcDeniedReply read(int xid, ReplyState replyState, XDR xdr) {
    Verifier verifier = Verifier.readFlavorAndVerifier(xdr);
    RejectState rejectState = RejectState.fromValue(xdr.readInt());
    return new RpcDeniedReply(xid, replyState, rejectState, verifier);
  }

  public RejectState getRejectState() {
    return rejectState;
  }
  
  @Override
  public String toString() {
    return new StringBuffer().append("xid:").append(xid)
        .append(",messageType:").append(messageType).append("verifier_flavor:")
        .append(verifier.getFlavor()).append("rejectState:")
        .append(rejectState).toString();
  }
  
  @Override
  public XDR write(XDR xdr) {
    xdr.writeInt(xid);
    xdr.writeInt(messageType.getValue());
    xdr.writeInt(replyState.getValue());
    Verifier.writeFlavorAndVerifier(verifier, xdr);
    xdr.writeInt(rejectState.getValue());
    return xdr;
  }
}

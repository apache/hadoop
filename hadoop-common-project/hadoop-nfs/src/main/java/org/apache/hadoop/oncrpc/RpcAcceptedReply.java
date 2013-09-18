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
 * Represents RPC message MSG_ACCEPTED reply body. See RFC 1831 for details.
 * This response is sent to a request to indicate success of the request.
 */
public class RpcAcceptedReply extends RpcReply {
  public enum AcceptState {
    // the order of the values below are significant.
    SUCCESS, /* RPC executed successfully */
    PROG_UNAVAIL, /* remote hasn't exported program */
    PROG_MISMATCH, /* remote can't support version # */
    PROC_UNAVAIL, /* program can't support procedure */
    GARBAGE_ARGS, /* procedure can't decode params */
    SYSTEM_ERR; /* e.g. memory allocation failure */
    
    public static AcceptState fromValue(int value) {
      return values()[value];
    }

    public int getValue() {
      return ordinal();
    }
  };
  
  public static RpcAcceptedReply getAcceptInstance(int xid, 
      Verifier verifier) {
    return getInstance(xid, AcceptState.SUCCESS, verifier);
  }
  
  public static RpcAcceptedReply getInstance(int xid, AcceptState state,
      Verifier verifier) {
    return new RpcAcceptedReply(xid, ReplyState.MSG_ACCEPTED, verifier,
        state);
  }

  private final AcceptState acceptState;

  RpcAcceptedReply(int xid, ReplyState state, Verifier verifier,
      AcceptState acceptState) {
    super(xid, state, verifier);
    this.acceptState = acceptState;
  }

  public static RpcAcceptedReply read(int xid, ReplyState replyState, XDR xdr) {
    Verifier verifier = Verifier.readFlavorAndVerifier(xdr);
    AcceptState acceptState = AcceptState.fromValue(xdr.readInt());
    return new RpcAcceptedReply(xid, replyState, verifier, acceptState);
  }

  public AcceptState getAcceptState() {
    return acceptState;
  }
  
  @Override
  public XDR write(XDR xdr) {
    xdr.writeInt(xid);
    xdr.writeInt(messageType.getValue());
    xdr.writeInt(replyState.getValue());
    Verifier.writeFlavorAndVerifier(verifier, xdr);
    xdr.writeInt(acceptState.getValue());
    return xdr;
  }
}

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

import org.apache.hadoop.oncrpc.RpcAcceptedReply.AcceptState;
import org.apache.hadoop.oncrpc.RpcAuthInfo.AuthFlavor;
import org.apache.hadoop.oncrpc.RpcReply.ReplyState;
import org.junit.Test;

/**
 * Test for {@link RpcAcceptedReply}
 */
public class TestRpcAcceptedReply {
  @Test
  public void testAcceptState() {
    assertEquals(AcceptState.SUCCESS, AcceptState.fromValue(0));
    assertEquals(AcceptState.PROG_UNAVAIL, AcceptState.fromValue(1));
    assertEquals(AcceptState.PROG_MISMATCH, AcceptState.fromValue(2));
    assertEquals(AcceptState.PROC_UNAVAIL, AcceptState.fromValue(3));
    assertEquals(AcceptState.GARBAGE_ARGS, AcceptState.fromValue(4));
    assertEquals(AcceptState.SYSTEM_ERR, AcceptState.fromValue(5));
  }
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void testAcceptStateFromInvalidValue() {
    AcceptState.fromValue(6);
  }
  
  @Test
  public void testConstructor() {
    RpcAuthInfo verifier = new RpcAuthInfo(AuthFlavor.AUTH_NONE, new byte[0]);
    RpcAcceptedReply reply = new RpcAcceptedReply(0, RpcMessage.RPC_REPLY,
        ReplyState.MSG_ACCEPTED, verifier, AcceptState.SUCCESS);
    assertEquals(0, reply.getXid());
    assertEquals(RpcMessage.RPC_REPLY, reply.getMessageType());
    assertEquals(ReplyState.MSG_ACCEPTED, reply.getState());
    assertEquals(verifier, reply.getVerifier());
    assertEquals(AcceptState.SUCCESS, reply.getAcceptState());
  }
}


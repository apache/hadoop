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


import org.apache.hadoop.oncrpc.RpcReply.ReplyState;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link RpcReply}
 */
public class TestRpcReply {
  @Test
  public void testReplyStateFromValue() {
    Assert.assertEquals(ReplyState.MSG_ACCEPTED, ReplyState.fromValue(0));
    Assert.assertEquals(ReplyState.MSG_DENIED, ReplyState.fromValue(1));
  }
  
  @Test(expected=IndexOutOfBoundsException.class)
  public void testReplyStateFromInvalidValue1() {
    ReplyState.fromValue(2);
  }
  
  @Test
  public void testRpcReply() {
    RpcReply reply = new RpcReply(0, ReplyState.MSG_ACCEPTED,
        new VerifierNone()) {
          @Override
          public XDR write(XDR xdr) {
            return null;
          }
    };
    Assert.assertEquals(0, reply.getXid());
    Assert.assertEquals(RpcMessage.Type.RPC_REPLY, reply.getMessageType());
    Assert.assertEquals(ReplyState.MSG_ACCEPTED, reply.getState());
  }
}

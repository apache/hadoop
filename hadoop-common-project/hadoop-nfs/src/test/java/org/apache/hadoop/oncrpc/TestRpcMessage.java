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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link RpcMessage}
 */
public class TestRpcMessage {
  private RpcMessage getRpcMessage(int xid, int msgType) {
    return new RpcMessage(xid, msgType) {
      // Anonymous class
    };
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidMessageType() {
    int invalidMsgType = 2; // valid values are 0 and 1
    getRpcMessage(0, invalidMsgType);
  }
  
  @Test
  public void testRpcMessage() {
    RpcMessage msg = getRpcMessage(0, RpcMessage.RPC_CALL);
    Assert.assertEquals(0, msg.getXid());
    Assert.assertEquals(RpcMessage.RPC_CALL, msg.getMessageType());
  }
  
  @Test
  public void testValidateMessage() {
    RpcMessage msg = getRpcMessage(0, RpcMessage.RPC_CALL);
    msg.validateMessageType(RpcMessage.RPC_CALL);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testValidateMessageException() {
    RpcMessage msg = getRpcMessage(0, RpcMessage.RPC_CALL);
    msg.validateMessageType(RpcMessage.RPC_REPLY);
  }
}

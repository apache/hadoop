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

import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.Credentials;
import org.apache.hadoop.oncrpc.security.Verifier;
import org.apache.hadoop.oncrpc.security.VerifierNone;
import org.junit.Test;

/**
 * Tests for {@link RpcCall}
 */
public class TestRpcCall {
  
  @Test
  public void testConstructor() {
    Credentials credential = new CredentialsNone();
    Verifier verifier = new VerifierNone();
    int rpcVersion = RpcCall.RPC_VERSION;
    int program = 2;
    int version = 3;
    int procedure = 4;
    RpcCall call = new RpcCall(0, RpcMessage.Type.RPC_CALL, rpcVersion, program,
        version, procedure, credential, verifier);
    assertEquals(0, call.getXid());
    assertEquals(RpcMessage.Type.RPC_CALL, call.getMessageType());
    assertEquals(rpcVersion, call.getRpcVersion());
    assertEquals(program, call.getProgram());
    assertEquals(version, call.getVersion());
    assertEquals(procedure, call.getProcedure());
    assertEquals(credential, call.getCredential());
    assertEquals(verifier, call.getVerifier());
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidRpcVersion() {
    int invalidRpcVersion = 3;
    new RpcCall(0, RpcMessage.Type.RPC_CALL, invalidRpcVersion, 2, 3, 4, null, null);
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidRpcMessageType() {
    RpcMessage.Type invalidMessageType = RpcMessage.Type.RPC_REPLY; // Message typ is not RpcMessage.RPC_CALL
    new RpcCall(0, invalidMessageType, RpcCall.RPC_VERSION, 2, 3, 4, null, null);
  }
}

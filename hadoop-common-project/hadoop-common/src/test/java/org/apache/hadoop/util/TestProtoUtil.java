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
package org.apache.hadoop.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.junit.Test;

import com.google.protobuf.CodedOutputStream;

public class TestProtoUtil {
  
  /**
   * Values to test encoding as variable length integers
   */
  private static final int[] TEST_VINT_VALUES = new int[] {
    0, 1, -1, 127, 128, 129, 255, 256, 257,
    0x1234, -0x1234,
    0x123456, -0x123456,
    0x12345678, -0x12345678
  };

  /**
   * Test that readRawVarint32 is compatible with the varints encoded
   * by ProtoBuf's CodedOutputStream.
   */
  @Test
  public void testVarInt() throws IOException {
    // Test a few manufactured values
    for (int value : TEST_VINT_VALUES) {
      doVarIntTest(value);
    }
    // Check 1-bits at every bit position
    for (int i = 1; i != 0; i <<= 1) {
      doVarIntTest(i);
      doVarIntTest(-i);
      doVarIntTest(i - 1);
      doVarIntTest(~i);
    }
  }
  
  private void doVarIntTest(int value) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream cout = CodedOutputStream.newInstance(baos);
    cout.writeRawVarint32(value);
    cout.flush();

    DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(value, ProtoUtil.readRawVarint32(dis));
  }
  
  @Test
  public void testRpcClientId() {
    byte[] uuid = ClientId.getClientId();
    RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
        RpcKind.RPC_PROTOCOL_BUFFER, OperationProto.RPC_FINAL_PACKET, 0,
        RpcConstants.INVALID_RETRY_COUNT, uuid);
    assertTrue(Arrays.equals(uuid, header.getClientId().toByteArray()));
  }
}

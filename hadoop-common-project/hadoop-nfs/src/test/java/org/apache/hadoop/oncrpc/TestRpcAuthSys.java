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

import org.junit.Test;

/**
 * Test for {@link RpcAuthSys}
 */
public class TestRpcAuthSys {
  @Test
  public void testConstructor() {
    RpcAuthSys auth = new RpcAuthSys(0, 1);
    assertEquals(0, auth.getUid());
    assertEquals(1, auth.getGid());
  }
  
  @Test
  public void testRead() {
    byte[] bytes = {0, 1, 2, 3}; // 4 bytes Stamp
    bytes = XDR.append(bytes, XDR.getVariableOpque(new byte[0]));
    bytes = XDR.append(bytes, XDR.toBytes(0)); // gid
    bytes = XDR.append(bytes, XDR.toBytes(1)); // uid
    RpcAuthSys auth = RpcAuthSys.from(bytes);
    assertEquals(0, auth.getUid());
    assertEquals(1, auth.getGid());
  }
}

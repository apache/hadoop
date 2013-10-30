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
package org.apache.hadoop.oncrpc.security;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.oncrpc.security.RpcAuthInfo;
import org.apache.hadoop.oncrpc.security.RpcAuthInfo.AuthFlavor;
import org.junit.Test;

/**
 * Tests for {@link RpcAuthInfo}
 */
public class TestRpcAuthInfo {
  @Test
  public void testAuthFlavor() {
    assertEquals(AuthFlavor.AUTH_NONE, AuthFlavor.fromValue(0));
    assertEquals(AuthFlavor.AUTH_SYS, AuthFlavor.fromValue(1));
    assertEquals(AuthFlavor.AUTH_SHORT, AuthFlavor.fromValue(2));
    assertEquals(AuthFlavor.AUTH_DH, AuthFlavor.fromValue(3));
    assertEquals(AuthFlavor.RPCSEC_GSS, AuthFlavor.fromValue(6));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testInvalidAuthFlavor() {
    assertEquals(AuthFlavor.AUTH_NONE, AuthFlavor.fromValue(4));
  }
}

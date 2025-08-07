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
package org.apache.hadoop.security;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuthorizationContext {

  @Test
  public void testSetAndGetAuthorizationHeader() {
    byte[] header = "my-auth-header".getBytes();
    AuthorizationContext.setCurrentAuthorizationHeader(header);
    Assertions.assertArrayEquals(header, AuthorizationContext.getCurrentAuthorizationHeader());
    AuthorizationContext.clear();
  }

  @Test
  public void testClearAuthorizationHeader() {
    byte[] header = "clear-me".getBytes();
    AuthorizationContext.setCurrentAuthorizationHeader(header);
    AuthorizationContext.clear();
    Assertions.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
  }

  @Test
  public void testThreadLocalIsolation() throws Exception {
    byte[] mainHeader = "main-thread".getBytes();
    AuthorizationContext.setCurrentAuthorizationHeader(mainHeader);
    Thread t = new Thread(() -> {
      Assertions.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
      byte[] threadHeader = "other-thread".getBytes();
      AuthorizationContext.setCurrentAuthorizationHeader(threadHeader);
      Assertions.assertArrayEquals(threadHeader, AuthorizationContext.getCurrentAuthorizationHeader());
      AuthorizationContext.clear();
      Assertions.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
    });
    t.start();
    t.join();
    // Main thread should still have its header
    Assertions.assertArrayEquals(mainHeader, AuthorizationContext.getCurrentAuthorizationHeader());
    AuthorizationContext.clear();
  }

  @Test
  public void testNullAndEmptyHeader() {
    AuthorizationContext.setCurrentAuthorizationHeader(null);
    Assertions.assertNull(AuthorizationContext.getCurrentAuthorizationHeader());
    byte[] empty = new byte[0];
    AuthorizationContext.setCurrentAuthorizationHeader(empty);
    Assertions.assertArrayEquals(empty, AuthorizationContext.getCurrentAuthorizationHeader());
    AuthorizationContext.clear();
  }
}
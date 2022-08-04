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
package org.apache.hadoop.net;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class TestInetAddressUtils {

  @Test
  public void testGetCanonicalHostName() throws UnknownHostException {
    InetAddress localhost = InetAddress.getLocalHost();
    InetAddress unresolved = InetAddress.getByAddress(localhost.getHostAddress(),
        localhost.getAddress());

    // Precondition: host name and canonical host name for unresolved returns an IP address.
    assertEquals(localhost.getHostAddress(), unresolved.getHostName());

    // Test: Get the canonical name despite InetAddress caching
    String canonicalHostName = InetAddressUtils.getCanonicalHostName(unresolved);

    // Verify: The canonical host name doesn't match the host address but does match the localhost.
    assertNotEquals(localhost.getHostAddress(), canonicalHostName);
    assertEquals(localhost.getCanonicalHostName(), canonicalHostName);
  }

}

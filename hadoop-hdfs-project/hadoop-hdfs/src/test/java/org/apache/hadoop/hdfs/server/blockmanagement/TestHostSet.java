/*
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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HostSet}.
 */
public class TestHostSet {

  /**
   * HDFS-17891: HostSet.add() must silently skip unresolved addresses
   * instead of throwing IllegalArgumentException.
   *
   * Regression test: before the fix, adding an unresolved InetSocketAddress
   * caused an uncaught exception that broke dfsadmin -report and the
   * NameNode web UI whenever any DataNode hostname was unresolvable.
   */
  @Test
  public void testAddUnresolvedAddressDoesNotThrow() {
    HostSet hostSet = new HostSet();
    InetSocketAddress unresolved =
        InetSocketAddress.createUnresolved("does-not-exist.example.local", 50010);

    assertTrue(unresolved.isUnresolved(), "Hostname should be unresolved");

    // Must not throw IllegalArgumentException (the pre-fix behaviour).
    hostSet.add(unresolved);

    // The unresolved address should be silently skipped, not added.
    assertEquals(0, hostSet.size(), "HostSet should be empty after adding an unresolved address");
  }

  /**
   * A resolved address must still be added normally. The fix must not
   * regress the happy path.
   */
  @Test
  public void testAddResolvedAddressSucceeds() {
    HostSet hostSet = new HostSet();
    // 127.0.0.1 is always resolvable without network access.
    InetSocketAddress resolved =
        new InetSocketAddress("127.0.0.1", 50010);

    assertFalse(resolved.isUnresolved(), "Hostname should be resolvable");

    hostSet.add(resolved);

    assertEquals(1, hostSet.size(), "HostSet should contain exactly one entry");
  }

  /**
   * Mixed batch: one unresolved address among resolved ones.
   * Only the resolved addresses should end up in the set; the unresolved one
   * must be skipped without aborting the whole operation.
   *
   * This mirrors the real scenario (Kubernetes node deleted, one DataNode
   * DNS entry gone) where dfsadmin -report broke for the entire cluster.
   */
  @Test
  public void testAddMixedAddressesSkipsUnresolved() {
    HostSet hostSet = new HostSet();

    InetSocketAddress resolved1 = new InetSocketAddress("127.0.0.1", 50010);
    InetSocketAddress resolved2 = new InetSocketAddress("127.0.0.1", 50020);
    InetSocketAddress unresolved =
        InetSocketAddress.createUnresolved("dead-node.example.local", 50030);

    hostSet.add(resolved1);
    hostSet.add(unresolved);  // Must not throw or abort.
    hostSet.add(resolved2);

    assertEquals(2, hostSet.size(), "Only the two resolved addresses should be in the HostSet");
  }
}

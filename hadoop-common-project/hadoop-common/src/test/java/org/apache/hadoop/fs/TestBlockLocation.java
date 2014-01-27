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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class TestBlockLocation {

  private static final String[] EMPTY_STR_ARRAY = new String[0];

  private static void checkBlockLocation(final BlockLocation loc)
      throws Exception {
    checkBlockLocation(loc, 0, 0, false);
  }

  private static void checkBlockLocation(final BlockLocation loc,
      final long offset, final long length, final boolean corrupt)
      throws Exception {
    checkBlockLocation(loc, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY, EMPTY_STR_ARRAY,
        EMPTY_STR_ARRAY, offset, length, corrupt);
  }

  private static void checkBlockLocation(final BlockLocation loc,
      String[] names, String[] hosts, String[] cachedHosts,
      String[] topologyPaths, final long offset, final long length,
      final boolean corrupt) throws Exception {
    assertNotNull(loc.getHosts());
    assertNotNull(loc.getCachedHosts());
    assertNotNull(loc.getNames());
    assertNotNull(loc.getTopologyPaths());

    assertArrayEquals(hosts, loc.getHosts());
    assertArrayEquals(cachedHosts, loc.getCachedHosts());
    assertArrayEquals(names, loc.getNames());
    assertArrayEquals(topologyPaths, loc.getTopologyPaths());

    assertEquals(offset, loc.getOffset());
    assertEquals(length, loc.getLength());
    assertEquals(corrupt, loc.isCorrupt());
  }

  /**
   * Call all the constructors and verify the delegation is working properly
   */
  @Test(timeout = 5000)
  public void testBlockLocationConstructors() throws Exception {
    //
    BlockLocation loc;
    loc = new BlockLocation();
    checkBlockLocation(loc);
    loc = new BlockLocation(null, null, 1, 2);
    checkBlockLocation(loc, 1, 2, false);
    loc = new BlockLocation(null, null, null, 1, 2);
    checkBlockLocation(loc, 1, 2, false);
    loc = new BlockLocation(null, null, null, 1, 2, true);
    checkBlockLocation(loc, 1, 2, true);
    loc = new BlockLocation(null, null, null, null, 1, 2, true);
    checkBlockLocation(loc, 1, 2, true);
  }

  /**
   * Call each of the setters and verify
   */
  @Test(timeout = 5000)
  public void testBlockLocationSetters() throws Exception {
    BlockLocation loc;
    loc = new BlockLocation();
    // Test that null sets the empty array
    loc.setHosts(null);
    loc.setCachedHosts(null);
    loc.setNames(null);
    loc.setTopologyPaths(null);
    checkBlockLocation(loc);
    // Test that not-null gets set properly
    String[] names = new String[] { "name" };
    String[] hosts = new String[] { "host" };
    String[] cachedHosts = new String[] { "cachedHost" };
    String[] topologyPaths = new String[] { "path" };
    loc.setNames(names);
    loc.setHosts(hosts);
    loc.setCachedHosts(cachedHosts);
    loc.setTopologyPaths(topologyPaths);
    loc.setOffset(1);
    loc.setLength(2);
    loc.setCorrupt(true);
    checkBlockLocation(loc, names, hosts, cachedHosts, topologyPaths, 1, 2,
        true);
  }
}

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

package org.apache.hadoop.yarn.server.resourcemanager.blacklist;


import java.util.Collections;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.junit.Assert;
import org.junit.Test;

public class TestBlacklistManager {

  @Test
  public void testSimpleBlacklistBelowFailureThreshold() {
    final int numberOfNodeManagerHosts = 3;
    final double blacklistDisableFailureThreshold = 0.8;
    BlacklistManager manager = new SimpleBlacklistManager(
        numberOfNodeManagerHosts, blacklistDisableFailureThreshold);
    String anyNode = "foo";
    String anyNode2 = "bar";
    manager.addNode(anyNode);
    manager.addNode(anyNode2);
    ResourceBlacklistRequest blacklist = manager
        .getBlacklistUpdates();

    List<String> blacklistAdditions = blacklist.getBlacklistAdditions();
    Collections.sort(blacklistAdditions);
    List<String> blacklistRemovals = blacklist.getBlacklistRemovals();
    String[] expectedBlacklistAdditions = new String[]{anyNode2, anyNode};
    Assert.assertArrayEquals(
        "Blacklist additions was not as expected",
        expectedBlacklistAdditions,
        blacklistAdditions.toArray());
    Assert.assertTrue(
        "Blacklist removals should be empty but was " +
            blacklistRemovals,
        blacklistRemovals.isEmpty());
  }

  @Test
  public void testSimpleBlacklistAboveFailureThreshold() {
    // Create a threshold of 0.5 * 3 i.e at 1.5 node failures.
    BlacklistManager manager = new SimpleBlacklistManager(3, 0.5);
    String anyNode = "foo";
    String anyNode2 = "bar";
    manager.addNode(anyNode);
    ResourceBlacklistRequest blacklist = manager
        .getBlacklistUpdates();

    List<String> blacklistAdditions = blacklist.getBlacklistAdditions();
    Collections.sort(blacklistAdditions);
    List<String> blacklistRemovals = blacklist.getBlacklistRemovals();
    String[] expectedBlacklistAdditions = new String[]{anyNode};
    Assert.assertArrayEquals(
        "Blacklist additions was not as expected",
        expectedBlacklistAdditions,
        blacklistAdditions.toArray());
    Assert.assertTrue(
        "Blacklist removals should be empty but was " +
            blacklistRemovals,
        blacklistRemovals.isEmpty());

    manager.addNode(anyNode2);

    blacklist = manager
        .getBlacklistUpdates();
    blacklistAdditions = blacklist.getBlacklistAdditions();
    Collections.sort(blacklistAdditions);
    blacklistRemovals = blacklist.getBlacklistRemovals();
    Collections.sort(blacklistRemovals);
    String[] expectedBlacklistRemovals = new String[] {anyNode2, anyNode};
    Assert.assertTrue(
        "Blacklist additions should be empty but was " +
            blacklistAdditions,
        blacklistAdditions.isEmpty());
    Assert.assertArrayEquals(
        "Blacklist removals was not as expected",
        expectedBlacklistRemovals,
        blacklistRemovals.toArray());
  }

  @Test
  public void testDisabledBlacklist() {
    BlacklistManager disabled = new DisabledBlacklistManager();
    String anyNode = "foo";
    disabled.addNode(anyNode);
    ResourceBlacklistRequest blacklist = disabled
        .getBlacklistUpdates();

    List<String> blacklistAdditions = blacklist.getBlacklistAdditions();
    List<String> blacklistRemovals = blacklist.getBlacklistRemovals();
    Assert.assertTrue(
        "Blacklist additions should be empty but was " +
            blacklistAdditions,
        blacklistAdditions.isEmpty());
    Assert.assertTrue(
        "Blacklist removals should be empty but was " +
            blacklistRemovals,
        blacklistRemovals.isEmpty());
  }
}

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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestLeaderFollowerResolver {
  @Test
  public void testResolve() throws Exception {
    // Mock the subcluster mapping
    Configuration conf = new Configuration();
    Router router = mock(Router.class);
    LeaderFollowerResolver leaderFollowerResolver = new LeaderFollowerResolver();

    // Add the mocks to the resolver
    MultipleDestinationMountTableResolver resolver =
        new MultipleDestinationMountTableResolver(conf, router);
    resolver.addResolver(DestinationOrder.LEADER_FOLLOWER, leaderFollowerResolver);

    Map<String, String> mapLocal = new LinkedHashMap<>();
    mapLocal.put("subcluster2", "/local");
    mapLocal.put("subcluster0", "/local");
    mapLocal.put("subcluster1", "/local");
    MountTable localEntry = MountTable.newInstance("/local", mapLocal);
    localEntry.setDestOrder(DestinationOrder.LEADER_FOLLOWER);
    resolver.addEntry(localEntry);

    PathLocation dest = resolver.getDestinationForPath("/local/file0.txt");
    assertDestination("subcluster2", dest);

  }

  private static void assertDestination(String expectedNsId, PathLocation loc) {
    List<RemoteLocation> dests = loc.getDestinations();
    RemoteLocation dest = dests.get(0);
    assertEquals(expectedNsId, dest.getNameserviceId());
  }
}

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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import static org.apache.hadoop.hdfs.server.federation.resolver.order.AvailableSpaceResolver.BALANCER_PREFERENCE_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.resolver.order.AvailableSpaceResolver.BALANCER_PREFERENCE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.AvailableSpaceResolver.SubclusterAvailableSpace;
import org.apache.hadoop.hdfs.server.federation.resolver.order.AvailableSpaceResolver.SubclusterSpaceComparator;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.MembershipStatsPBImpl;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

/**
 * Test the {@link AvailableSpaceResolver}.
 */
public class TestAvailableSpaceResolver {

  private static final int SUBCLUSTER_NUM = 10;

  @Test
  public void testResolverWithNoPreference() throws IOException {
    MultipleDestinationMountTableResolver mountTableResolver =
        mockAvailableSpaceResolver(1.0f);
    // Since we don't have any preference, it will
    // always chose the maximum-available-space subcluster.
    PathLocation loc = mountTableResolver.getDestinationForPath("/space");
    assertEquals("subcluster9",
        loc.getDestinations().get(0).getNameserviceId());

    loc = mountTableResolver.getDestinationForPath("/space/subdir");
    assertEquals("subcluster9",
        loc.getDestinations().get(0).getNameserviceId());
  }

  @Test
  public void testResolverWithDefaultPreference() throws IOException {
    MultipleDestinationMountTableResolver mountTableResolver =
        mockAvailableSpaceResolver(BALANCER_PREFERENCE_DEFAULT);

    int retries = 10;
    int retryTimes = 0;
    // There is chance we won't always chose the
    // maximum-available-space subcluster.
    for (retryTimes = 0; retryTimes < retries; retryTimes++) {
      PathLocation loc = mountTableResolver.getDestinationForPath("/space");
      if (!"subcluster9"
          .equals(loc.getDestinations().get(0).getNameserviceId())) {
        break;
      }
    }
    assertNotEquals(retries, retryTimes);
  }

  /**
   * Mock the available space based resolver.
   *
   * @param balancerPreference The balancer preference for the resolver.
   * @throws IOException
   * @return MultipleDestinationMountTableResolver instance.
   */
  @SuppressWarnings("unchecked")
  private MultipleDestinationMountTableResolver mockAvailableSpaceResolver(
      float balancerPreference) throws IOException {
    Configuration conf = new Configuration();
    conf.setFloat(BALANCER_PREFERENCE_KEY, balancerPreference);
    Router router = mock(Router.class);
    StateStoreService stateStore = mock(StateStoreService.class);
    MembershipStore membership = mock(MembershipStore.class);
    when(router.getStateStore()).thenReturn(stateStore);
    when(stateStore.getRegisteredRecordStore(any(Class.class)))
        .thenReturn(membership);
    GetNamenodeRegistrationsResponse response =
        GetNamenodeRegistrationsResponse.newInstance();
    // Set the mapping for each client
    List<MembershipState> records = new LinkedList<>();
    for (int i = 0; i < SUBCLUSTER_NUM; i++) {
      records.add(newMembershipState("subcluster" + i, i));
    }
    response.setNamenodeMemberships(records);

    when(membership
        .getNamenodeRegistrations(any(GetNamenodeRegistrationsRequest.class)))
            .thenReturn(response);

    // construct available space resolver
    AvailableSpaceResolver resolver = new AvailableSpaceResolver(conf, router);
    MultipleDestinationMountTableResolver mountTableResolver =
        new MultipleDestinationMountTableResolver(conf, router);
    mountTableResolver.addResolver(DestinationOrder.SPACE, resolver);

    // We point /space to subclusters [0,..9] with the SPACE order
    Map<String, String> destinations = new HashMap<>();
    for (int i = 0; i < SUBCLUSTER_NUM; i++) {
      destinations.put("subcluster" + i, "/space");
    }
    MountTable spaceEntry = MountTable.newInstance("/space", destinations);
    spaceEntry.setDestOrder(DestinationOrder.SPACE);
    mountTableResolver.addEntry(spaceEntry);

    return mountTableResolver;
  }

  public static MembershipState newMembershipState(String nameservice,
      long availableSpace) {
    MembershipState record = MembershipState.newInstance();
    record.setNameserviceId(nameservice);

    MembershipStats stats = new MembershipStatsPBImpl();
    stats.setAvailableSpace(availableSpace);
    record.setStats(stats);
    return record;
  }

  @Test
  public void testSubclusterSpaceComparator() {
    verifyRank(0.0f, true, false);
    verifyRank(1.0f, true, true);
    verifyRank(0.5f, false, false);
    verifyRank(BALANCER_PREFERENCE_DEFAULT, false, false);

    // test for illegal cases
    try {
      verifyRank(2.0f, false, false);
      fail("Subcluster comparison should be failed.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "The balancer preference value should be in the range 0.0 - 1.0", e);
    }

    try {
      verifyRank(-1.0f, false, false);
      fail("Subcluster comparison should be failed.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "The balancer preference value should be in the range 0.0 - 1.0", e);
    }
  }

  /**
   * Verify result rank with {@link SubclusterSpaceComparator}.
   * @param balancerPreference The balancer preference used
   *        in {@link SubclusterSpaceComparator}.
   * @param shouldOrdered The result rank should be ordered.
   * @param isDesc If the rank result is in a descending order.
   */
  private void verifyRank(float balancerPreference, boolean shouldOrdered,
      boolean isDesc) {
    List<SubclusterAvailableSpace> subclusters = new LinkedList<>();
    for (int i = 0; i < SUBCLUSTER_NUM; i++) {
      subclusters.add(new SubclusterAvailableSpace("subcluster" + i, i));
    }

    // shuffle the cluster list if we expect rank to be ordered
    if (shouldOrdered) {
      Collections.shuffle(subclusters);
    }

    SubclusterSpaceComparator comparator = new SubclusterSpaceComparator(
        balancerPreference);
    Collections.sort(subclusters, comparator);

    int i = SUBCLUSTER_NUM - 1;
    for (; i >= 0; i--) {
      SubclusterAvailableSpace cluster = subclusters
          .get(SUBCLUSTER_NUM - 1 - i);

      if (shouldOrdered) {
        if (isDesc) {
          assertEquals("subcluster" + i, cluster.getNameserviceId());
          assertEquals(i, cluster.getAvailableSpace());
        } else {
          assertEquals("subcluster" + (SUBCLUSTER_NUM - 1 - i),
              cluster.getNameserviceId());
          assertEquals(SUBCLUSTER_NUM - 1 - i, cluster.getAvailableSpace());
        }
      } else {
        // If catch one cluster is not in ordered, that's expected behavior.
        if (!cluster.getNameserviceId().equals("subcluster" + i)
            && cluster.getAvailableSpace() != i) {
          break;
        }
      }
    }

    // The var i won't reach to 0 since cluster list won't be completely
    // ordered.
    if (!shouldOrdered) {
      assertNotEquals(0, i);
    }
    subclusters.clear();
  }
}

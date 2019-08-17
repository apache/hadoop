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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the {@link LocalResolver}.
 */
public class TestLocalResolver {

  @Test
  @SuppressWarnings("unchecked")
  public void testLocalResolver() throws IOException {

    // Mock the subcluster mapping
    Configuration conf = new Configuration();
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
    records.add(newMembershipState("client0", "subcluster0"));
    records.add(newMembershipState("client1", "subcluster1"));
    records.add(newMembershipState("client2", "subcluster2"));
    response.setNamenodeMemberships(records);
    when(membership.getNamenodeRegistrations(
        any(GetNamenodeRegistrationsRequest.class))).thenReturn(response);

    // Mock the client resolution: it will be anything in sb
    StringBuilder sb = new StringBuilder("clientX");
    LocalResolver localResolver = new LocalResolver(conf, router);
    LocalResolver spyLocalResolver = spy(localResolver);
    doAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        return sb.toString();
      }
    }).when(spyLocalResolver).getClientAddr();

    // Add the mocks to the resolver
    MultipleDestinationMountTableResolver resolver =
        new MultipleDestinationMountTableResolver(conf, router);
    resolver.addResolver(DestinationOrder.LOCAL, spyLocalResolver);


    // We point /local to subclusters 0, 1, 2 with the local order
    Map<String, String> mapLocal = new HashMap<>();
    mapLocal.put("subcluster0", "/local");
    mapLocal.put("subcluster1", "/local");
    mapLocal.put("subcluster2", "/local");
    MountTable localEntry = MountTable.newInstance("/local", mapLocal);
    localEntry.setDestOrder(DestinationOrder.LOCAL);
    resolver.addEntry(localEntry);

    // Test first with the default destination
    PathLocation dest = resolver.getDestinationForPath("/local/file0.txt");
    assertDestination("subcluster0", dest);

    // We change the client location and verify
    setClient(sb, "client2");
    dest = resolver.getDestinationForPath("/local/file0.txt");
    assertDestination("subcluster2", dest);

    setClient(sb, "client1");
    dest = resolver.getDestinationForPath("/local/file0.txt");
    assertDestination("subcluster1", dest);

    setClient(sb, "client0");
    dest = resolver.getDestinationForPath("/local/file0.txt");
    assertDestination("subcluster0", dest);
  }

  private void assertDestination(String expectedNsId, PathLocation loc) {
    List<RemoteLocation> dests = loc.getDestinations();
    RemoteLocation dest = dests.get(0);
    assertEquals(expectedNsId, dest.getNameserviceId());
  }

  private MembershipState newMembershipState(String addr, String nsId) {
    return MembershipState.newInstance(
        "routerId", nsId, "nn0", "cluster0", "blockPool0",
        addr + ":8001", addr + ":8002", addr + ":8003", addr + ":8004",
        FederationNamenodeServiceState.ACTIVE, false);
  }

  /**
   * Set the address of the client issuing the request. We use a StringBuilder
   * to modify the value in place for the mock.
   * @param sb StringBuilder to set the client string.
   * @param client Address of the client.
   */
  private static void setClient(StringBuilder sb, String client) {
    sb.replace(0, sb.length(), client);
  }
}

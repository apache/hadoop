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

package org.apache.hadoop.yarn.client.api.impl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.InvalidContainerRequestException;
import org.junit.Test;

public class TestAMRMClientContainerRequest {

  @Test
  public void testOpportunisticAndGuaranteedRequests() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();

    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);

    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request =
        ContainerRequest.newBuilder().capability(capability)
            .nodes(new String[] { "host1", "host2" })
            .racks(new String[] { "/rack2" }).priority(Priority.newInstance(1))
            .build();
    client.addContainerRequest(request);
    verifyResourceRequest(client, request, "host1", true);
    verifyResourceRequest(client, request, "host2", true);
    verifyResourceRequest(client, request, "/rack1", true);
    verifyResourceRequest(client, request, "/rack2", true);
    verifyResourceRequest(client, request, ResourceRequest.ANY, true);
    ContainerRequest request2 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            new String[] {"/rack2"}, Priority.newInstance(1), 0, true, null,
            ExecutionTypeRequest.newInstance(
                ExecutionType.OPPORTUNISTIC, true));
    client.addContainerRequest(request2);
    verifyResourceRequest(client, request, "host1", true,
        ExecutionType.OPPORTUNISTIC);
    verifyResourceRequest(client, request, "host2", true,
        ExecutionType.OPPORTUNISTIC);
    verifyResourceRequest(client, request, "/rack1", true,
        ExecutionType.OPPORTUNISTIC);
    verifyResourceRequest(client, request, "/rack2", true,
        ExecutionType.OPPORTUNISTIC);
    verifyResourceRequest(client, request, ResourceRequest.ANY, true,
        ExecutionType.OPPORTUNISTIC);
  }

  @Test
  public void testFillInRacks() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
 
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            new String[] {"/rack2"}, Priority.newInstance(1));
    client.addContainerRequest(request);
    verifyResourceRequest(client, request, "host1", true);
    verifyResourceRequest(client, request, "host2", true);
    verifyResourceRequest(client, request, "/rack1", true);
    verifyResourceRequest(client, request, "/rack2", true);
    verifyResourceRequest(client, request, ResourceRequest.ANY, true);
  }
  
  @Test
  public void testDisableLocalityRelaxation() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest nodeLevelRequest =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(nodeLevelRequest);

    verifyResourceRequest(client, nodeLevelRequest, ResourceRequest.ANY, false);
    verifyResourceRequest(client, nodeLevelRequest, "/rack1", false);
    verifyResourceRequest(client, nodeLevelRequest, "host1", true);
    verifyResourceRequest(client, nodeLevelRequest, "host2", true);
    
    // Make sure we don't get any errors with two node-level requests at the
    // same priority
    ContainerRequest nodeLevelRequest2 =
        new ContainerRequest(capability, new String[] {"host2", "host3"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(nodeLevelRequest2);
    
    AMRMClient.ContainerRequest rackLevelRequest =
        new AMRMClient.ContainerRequest(capability, null,
            new String[] {"/rack3", "/rack4"}, Priority.newInstance(2), false);
    client.addContainerRequest(rackLevelRequest);
    
    verifyResourceRequest(client, rackLevelRequest, ResourceRequest.ANY, false);
    verifyResourceRequest(client, rackLevelRequest, "/rack3", true);
    verifyResourceRequest(client, rackLevelRequest, "/rack4", true);
    
    // Make sure we don't get any errors with two rack-level requests at the
    // same priority
    AMRMClient.ContainerRequest rackLevelRequest2 =
        new AMRMClient.ContainerRequest(capability, null,
            new String[] {"/rack4", "/rack5"}, Priority.newInstance(2), false);
    client.addContainerRequest(rackLevelRequest2);
    
    ContainerRequest bothLevelRequest =
        new ContainerRequest(capability, new String[] {"host3", "host4"},
            new String[] {"rack1", "/otherrack"},
            Priority.newInstance(3), false);
    client.addContainerRequest(bothLevelRequest);

    verifyResourceRequest(client, bothLevelRequest, ResourceRequest.ANY, false);
    verifyResourceRequest(client, bothLevelRequest, "rack1",
        true);
    verifyResourceRequest(client, bothLevelRequest, "/otherrack",
        true);
    verifyResourceRequest(client, bothLevelRequest, "host3", true);
    verifyResourceRequest(client, bothLevelRequest, "host4", true);
    
    // Make sure we don't get any errors with two both-level requests at the
    // same priority
    ContainerRequest bothLevelRequest2 =
        new ContainerRequest(capability, new String[] {"host4", "host5"},
            new String[] {"rack1", "/otherrack2"},
            Priority.newInstance(3), false);
    client.addContainerRequest(bothLevelRequest2);
  }
  
  @Test (expected = InvalidContainerRequestException.class)
  public void testDifferentLocalityRelaxationSamePriority() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request1 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request1);
    ContainerRequest request2 =
        new ContainerRequest(capability, new String[] {"host3"},
            null, Priority.newInstance(1), true);
    client.addContainerRequest(request2);
  }
  
  @Test
  public void testInvalidValidWhenOldRemoved() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request1 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request1);
    
    client.removeContainerRequest(request1);

    ContainerRequest request2 =
        new ContainerRequest(capability, new String[] {"host3"},
            null, Priority.newInstance(1), true);
    client.addContainerRequest(request2);
    
    client.removeContainerRequest(request2);
    
    ContainerRequest request3 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request3);
    
    client.removeContainerRequest(request3);
    
    ContainerRequest request4 =
        new ContainerRequest(capability, null,
            new String[] {"rack1"}, Priority.newInstance(1), true);
    client.addContainerRequest(request4);

  }
  
  @Test (expected = InvalidContainerRequestException.class)
  public void testLocalityRelaxationDifferentLevels() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request1 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request1);
    ContainerRequest request2 =
        new ContainerRequest(capability, null,
            new String[] {"rack1"}, Priority.newInstance(1), true);
    client.addContainerRequest(request2);
  }
  
  private static class MyResolver implements DNSToSwitchMapping {

    @Override
    public List<String> resolve(List<String> names) {
      return Arrays.asList("/rack1");
    }

    @Override
    public void reloadCachedMappings() {}

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
  }
  
  private void verifyResourceRequest(
      AMRMClientImpl<ContainerRequest> client, ContainerRequest request,
      String location, boolean expectedRelaxLocality) {
    verifyResourceRequest(client, request, location, expectedRelaxLocality,
        ExecutionType.GUARANTEED);
  }

  private void verifyResourceRequest(
      AMRMClientImpl<ContainerRequest> client, ContainerRequest request,
      String location, boolean expectedRelaxLocality,
      ExecutionType executionType) {
    ResourceRequest ask = client.getTable(0)
        .get(request.getPriority(), location, executionType,
            request.getCapability()).remoteRequest;
    assertEquals(location, ask.getResourceName());
    assertEquals(1, ask.getNumContainers());
    assertEquals(expectedRelaxLocality, ask.getRelaxLocality());
  }
}

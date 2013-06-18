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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestAMRMClientContainerRequest {
  @Test
  public void testFillInRacks() {
    AMRMClientImpl<ContainerRequest> client = new AMRMClientImpl<ContainerRequest>(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0l, 0), 0));
    
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
 
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            new String[] {"/rack2"}, Priority.newInstance(1), 4);
    client.addContainerRequest(request);
    verifyResourceRequestLocation(client, request, "host1");
    verifyResourceRequestLocation(client, request, "host2");
    verifyResourceRequestLocation(client, request, "/rack1");
    verifyResourceRequestLocation(client, request, "/rack2");
    verifyResourceRequestLocation(client, request, ResourceRequest.ANY);
  }
  
  private static class MyResolver implements DNSToSwitchMapping {

    @Override
    public List<String> resolve(List<String> names) {
      return Arrays.asList("/rack1");
    }

    @Override
    public void reloadCachedMappings() {}
  }
  
  private void verifyResourceRequestLocation(
      AMRMClientImpl<ContainerRequest> client, ContainerRequest request,
      String location) {
    ResourceRequest ask =  client.remoteRequestsTable.get(request.getPriority())
        .get(location).get(request.getCapability()).remoteRequest;
    assertEquals(location, ask.getResourceName());
    assertEquals(request.getContainerCount(), ask.getNumContainers());
  }
}

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

package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesRequestProto;
import org.apache.hadoop.yarn.service.MockRunningServiceContext;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.TestServiceManager;
import org.apache.hadoop.yarn.service.api.records.ComponentContainers;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFilterUtils {

  @RegisterExtension
  private ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testNoFilter() throws Exception {
    GetCompInstancesRequestProto req = GetCompInstancesRequestProto.newBuilder()
        .build();
    List<ComponentContainers> compContainers = FilterUtils.filterInstances(
        new MockRunningServiceContext(rule,
            TestServiceManager.createBaseDef("service")), req);
    assertEquals(2, compContainers.size(), "num comps");
    compContainers.forEach(item -> {
      assertEquals(2, item.getContainers().size(), "num containers");
    });
  }

  @Test
  public void testFilterWithComp() throws Exception {
    GetCompInstancesRequestProto req = GetCompInstancesRequestProto.newBuilder()
        .addAllComponentNames(Lists.newArrayList("compa")).build();
    List<ComponentContainers> compContainers = FilterUtils.filterInstances(
        new MockRunningServiceContext(rule,
            TestServiceManager.createBaseDef("service")), req);
    assertEquals(1, compContainers.size(), "num comps");
    assertEquals("compa", compContainers.get(0).getComponentName(), "comp name");
    assertEquals(2, compContainers.get(0).getContainers().size(), "num containers");
  }

  @Test
  public void testFilterWithVersion() throws Exception {
    ServiceContext sc = new MockRunningServiceContext(rule,
        TestServiceManager.createBaseDef("service"));
    GetCompInstancesRequestProto.Builder reqBuilder =
        GetCompInstancesRequestProto.newBuilder();

    reqBuilder.setVersion("v2");
    assertEquals(0, FilterUtils.filterInstances(sc, reqBuilder.build()).size(),
        "num comps");

    reqBuilder.addAllComponentNames(Lists.newArrayList("compa"))
        .setVersion("v1").build();

    assertEquals(2, FilterUtils.filterInstances(sc, reqBuilder.build()).get(0)
        .getContainers().size(), "num containers");
  }

  @Test
  public void testFilterWithState() throws Exception {
    ServiceContext sc = new MockRunningServiceContext(rule,
        TestServiceManager.createBaseDef("service"));
    GetCompInstancesRequestProto.Builder reqBuilder =
        GetCompInstancesRequestProto.newBuilder();

    reqBuilder.addAllContainerStates(Lists.newArrayList(
        ContainerState.READY.toString()));
    List<ComponentContainers> compContainers = FilterUtils.filterInstances(sc,
        reqBuilder.build());
    assertEquals(2, compContainers.size(), "num comps");
    compContainers.forEach(item -> {
      assertEquals(2, item.getContainers().size(), "num containers");
    });

    reqBuilder.clearContainerStates();
    reqBuilder.addAllContainerStates(Lists.newArrayList(
        ContainerState.STOPPED.toString()));
    assertEquals(0, FilterUtils.filterInstances(sc, reqBuilder.build()).size(),
        "num comps");
  }

}

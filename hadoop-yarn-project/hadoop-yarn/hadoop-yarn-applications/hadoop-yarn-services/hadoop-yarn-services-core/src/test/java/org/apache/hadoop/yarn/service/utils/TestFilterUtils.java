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

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetCompInstancesRequestProto;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.ServiceTestUtils;
import org.apache.hadoop.yarn.service.TestServiceManager;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.MockRunningServiceContext;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class TestFilterUtils {

  @Rule
  public ServiceTestUtils.ServiceFSWatcher rule =
      new ServiceTestUtils.ServiceFSWatcher();

  @Test
  public void testNoFilter() throws Exception {
    GetCompInstancesRequestProto req = GetCompInstancesRequestProto.newBuilder()
        .build();
    List<Container> containers = FilterUtils.filterInstances(
        new MockRunningServiceContext(rule,
            TestServiceManager.createBaseDef("service")), req);
    Assert.assertEquals("num containers", 4, containers.size());
  }

  @Test
  public void testFilterWithComp() throws Exception {
    GetCompInstancesRequestProto req = GetCompInstancesRequestProto.newBuilder()
        .addAllComponentNames(Lists.newArrayList("compa")).build();
    List<Container> containers = FilterUtils.filterInstances(
        new MockRunningServiceContext(rule,
            TestServiceManager.createBaseDef("service")), req);
    Assert.assertEquals("num containers", 2, containers.size());
  }

  @Test
  public void testFilterWithVersion() throws Exception {
    ServiceContext sc = new MockRunningServiceContext(rule,
        TestServiceManager.createBaseDef("service"));
    GetCompInstancesRequestProto.Builder reqBuilder =
        GetCompInstancesRequestProto.newBuilder();

    reqBuilder.setVersion("v2");
    Assert.assertEquals("num containers", 0,
        FilterUtils.filterInstances(sc, reqBuilder.build()).size());

    reqBuilder.addAllComponentNames(Lists.newArrayList("compa"))
        .setVersion("v1").build();

    Assert.assertEquals("num containers", 2,
        FilterUtils.filterInstances(sc, reqBuilder.build()).size());

    reqBuilder.setVersion("v2").build();
    Assert.assertEquals("num containers", 0,
        FilterUtils.filterInstances(sc, reqBuilder.build()).size());
  }

  @Test
  public void testFilterWithState() throws Exception {
    ServiceContext sc = new MockRunningServiceContext(rule,
        TestServiceManager.createBaseDef("service"));
    GetCompInstancesRequestProto.Builder reqBuilder =
        GetCompInstancesRequestProto.newBuilder();

    reqBuilder.addAllContainerStates(Lists.newArrayList(
        ContainerState.READY.toString()));
    Assert.assertEquals("num containers", 4,
        FilterUtils.filterInstances(sc, reqBuilder.build()).size());

    reqBuilder.clearContainerStates();
    reqBuilder.addAllContainerStates(Lists.newArrayList(
        ContainerState.STOPPED.toString()));
    Assert.assertEquals("num containers", 0,
        FilterUtils.filterInstances(sc, reqBuilder.build()).size());
  }

}

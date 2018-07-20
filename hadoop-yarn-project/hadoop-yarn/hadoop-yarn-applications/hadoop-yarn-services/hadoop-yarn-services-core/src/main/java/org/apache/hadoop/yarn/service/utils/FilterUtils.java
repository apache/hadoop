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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.ClientAMProtocol;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.api.records.Container;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterUtils {

  /**
   * Returns containers filtered by requested fields.
   *
   * @param context   service context
   * @param filterReq filter request
   */
  public static List<Container> filterInstances(ServiceContext context,
      ClientAMProtocol.GetCompInstancesRequestProto filterReq) {
    List<Container> results = new ArrayList<>();
    Map<ContainerId, ComponentInstance> instances =
        context.scheduler.getLiveInstances();

    instances.forEach(((containerId, instance) -> {
      boolean include = true;
      if (filterReq.getComponentNamesList() != null &&
          !filterReq.getComponentNamesList().isEmpty()) {
        // filter by component name
        if (!filterReq.getComponentNamesList().contains(
            instance.getComponent().getName())) {
          include = false;
        }
      }

      if (filterReq.getVersion() != null && !filterReq.getVersion().isEmpty()) {
        // filter by version
        String instanceServiceVersion = instance.getServiceVersion();
        if (instanceServiceVersion == null || !instanceServiceVersion.equals(
            filterReq.getVersion())) {
          include = false;
        }
      }

      if (filterReq.getContainerStatesList() != null &&
          !filterReq.getContainerStatesList().isEmpty()) {
        // filter by state
        if (!filterReq.getContainerStatesList().contains(
            instance.getContainerState().toString())) {
          include = false;
        }
      }

      if (include) {
        results.add(instance.getContainerSpec());
      }
    }));

    return results;
  }
}

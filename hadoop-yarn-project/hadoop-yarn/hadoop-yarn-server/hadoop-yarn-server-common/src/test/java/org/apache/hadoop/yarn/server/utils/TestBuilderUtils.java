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

package org.apache.hadoop.yarn.server.utils;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for BuilderUtils.
 */
public class TestBuilderUtils {
  @Test
  public void testNewAMResourceRequest() {
    final Priority priority = Priority.newInstance(0);
    final String hostName = "ResourceA";
    final Resource capacity = Resource.newInstance(1024, 1);

    ResourceRequest resourceRequest =
        BuilderUtils.newAMResourceRequest(priority, hostName, capacity);

    Assert.assertEquals("unexpected priority", priority,
        resourceRequest.getPriority());
    Assert.assertEquals("unexpected host name", hostName,
        resourceRequest.getResourceName());
    Assert.assertEquals("unexpected capacity", capacity,
        resourceRequest.getCapability());
    Assert.assertEquals("unexpected number of containers", 1,
        resourceRequest.getNumContainers());
    Assert.assertEquals("unexpected execution type",
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true),
        resourceRequest.getExecutionTypeRequest());
  }
}

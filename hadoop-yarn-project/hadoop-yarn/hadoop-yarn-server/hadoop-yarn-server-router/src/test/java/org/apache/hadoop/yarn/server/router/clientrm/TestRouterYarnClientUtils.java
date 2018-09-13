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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.util.ArrayList;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for RouterYarnClientUtils.
 */
public class TestRouterYarnClientUtils {

  @Test
  public void testClusterMetricsMerge() {
    ArrayList<GetClusterMetricsResponse> responses = new ArrayList<>();
    responses.add(getClusterMetricsResponse(1));
    responses.add(getClusterMetricsResponse(2));
    GetClusterMetricsResponse result = RouterYarnClientUtils.merge(responses);
    YarnClusterMetrics resultMetrics = result.getClusterMetrics();
    Assert.assertEquals(3, resultMetrics.getNumNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumActiveNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumDecommissionedNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumLostNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumRebootedNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumUnhealthyNodeManagers());
  }

  public GetClusterMetricsResponse getClusterMetricsResponse(int value) {
    YarnClusterMetrics metrics = YarnClusterMetrics.newInstance(value);
    metrics.setNumUnhealthyNodeManagers(value);
    metrics.setNumRebootedNodeManagers(value);
    metrics.setNumLostNodeManagers(value);
    metrics.setNumDecommissionedNodeManagers(value);
    metrics.setNumActiveNodeManagers(value);
    metrics.setNumNodeManagers(value);
    return GetClusterMetricsResponse.newInstance(metrics);
  }
}

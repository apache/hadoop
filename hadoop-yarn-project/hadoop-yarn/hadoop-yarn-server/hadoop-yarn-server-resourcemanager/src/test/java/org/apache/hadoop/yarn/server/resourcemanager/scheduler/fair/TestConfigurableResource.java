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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * To test class {@link ConfigurableResource}.
 */
public class TestConfigurableResource {
  private final Resource clusterResource = Resources.createResource(2048, 2);

  @Test
  public void testGetResourceWithPercentage() {
    ConfigurableResource configurableResource =
        new ConfigurableResource(new double[] {0.5, 0.5});
    assertEquals(
        configurableResource.getResource(clusterResource).getMemorySize(),
        1024);
    assertEquals(
        configurableResource.getResource(clusterResource).getVirtualCores(), 1);

    assertNull("The absolute resource should be null since object"
            + " configurableResource is initialized with percentages",
        configurableResource.getResource());
    assertNull("The absolute resource should be null since cluster resource"
        + " is null", configurableResource.getResource(null));
  }

  @Test
  public void testGetResourceWithAbsolute() {
    ConfigurableResource configurableResource =
        new ConfigurableResource(Resources.createResource(3072, 3));
    assertThat(configurableResource.getResource().getMemorySize()).
        isEqualTo(3072);
    assertThat(configurableResource.getResource().getVirtualCores()).
        isEqualTo(3);

    assertEquals(
        configurableResource.getResource(clusterResource).getMemorySize(),
        3072);
    assertEquals(
        configurableResource.getResource(clusterResource).getVirtualCores(),
        3);

    assertThat(configurableResource.getResource(null).getMemorySize()).
        isEqualTo(3072);
    assertThat(configurableResource.getResource(null).getVirtualCores()).
        isEqualTo(3);
  }
}

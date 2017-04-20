/*
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

package org.apache.slider.utils;

import org.apache.slider.api.resource.Application;
import org.junit.Test;

import java.util.Collections;

/**
 * Test for some of the command test base operations.
 */
public class TestAssertions {

  public static final String CLUSTER_JSON = "json/cluster.json";

  @Test
  public void testNoInstances() throws Throwable {
    Application application = new Application();
    application.setContainers(null);
    SliderTestUtils.assertContainersLive(application, "example", 0);
  }

  @Test
  public void testEmptyInstances() throws Throwable {
    Application application = new Application();
    application.setContainers(Collections.emptyList());
    SliderTestUtils.assertContainersLive(application, "example", 0);
  }

// TODO test metrics retrieval
//  @Test
//  public void testLiveInstances() throws Throwable {
//    InputStream stream = getClass().getClassLoader().getResourceAsStream(
//        CLUSTER_JSON);
//    assertNotNull("could not load " + CLUSTER_JSON, stream);
//    ClusterDescription liveCD = ClusterDescription.fromStream(stream);
//    assertNotNull(liveCD);
//    SliderTestUtils.assertContainersLive(liveCD, "SLEEP_LONG", 4);
//    assertEquals((Integer) 1, liveCD.statistics.get("SLEEP_LONG").get(
//        StatusKeys.STATISTICS_CONTAINERS_ANTI_AFFINE_PENDING));
//  }

}

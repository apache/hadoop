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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV1Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV2Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that the RM creates timeline services (v1/v2) as specified by the
 * configuration.
 */
public class TestRMTimelineService {
  private static MockRM rm;

  private void setup(boolean v1Enabled, boolean v2Enabled,
                     boolean systemMetricEnabled) {
    Configuration conf = new YarnConfiguration(new Configuration(false));
    Assert.assertFalse(YarnConfiguration.timelineServiceEnabled(conf));

    conf.setBoolean(YarnConfiguration.SYSTEM_METRICS_PUBLISHER_ENABLED,
        systemMetricEnabled);

    if (v1Enabled || v2Enabled) {
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    }

    if (v1Enabled) {
      conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.0f);
    }

    if (v2Enabled) {
      conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
          FileSystemTimelineWriterImpl.class, TimelineWriter.class);
    }

    if (v1Enabled && v2Enabled) {
      conf.set(YarnConfiguration.TIMELINE_SERVICE_VERSION, "1.0");
      conf.set(YarnConfiguration.TIMELINE_SERVICE_VERSIONS, "1.0,2.0f");
    }

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    rm = new MockRM(conf, memStore);
    rm.start();
  }

  // validate RM services exist or not as we specified
  private void validate(boolean v1Enabled, boolean v2Enabled,
                        boolean systemMetricEnabled) {
    boolean v1PublisherServiceFound = false;
    boolean v2PublisherServiceFound = false;
    List<Service> services = rm.getServices();
    for (Service service : services) {
      if (service instanceof TimelineServiceV1Publisher) {
        v1PublisherServiceFound = true;
      } else if (service instanceof TimelineServiceV2Publisher) {
        v2PublisherServiceFound = true;
      }
    }

    if(systemMetricEnabled) {
      Assert.assertEquals(v1Enabled, v1PublisherServiceFound);
      Assert.assertEquals(v2Enabled, v2PublisherServiceFound);
    } else {
      Assert.assertEquals(false, v1PublisherServiceFound);
      Assert.assertEquals(false, v2PublisherServiceFound);
    }
  }

  private void cleanup() throws Exception {
    rm.close();
    rm.stop();
  }

  // runs test to validate RM creates a timeline service publisher if and
  // only if the service is enabled for v1 and v2 (independently).
  private void runTest(boolean v1Enabled, boolean v2Enabled,
                       boolean systemMetricEnabled) throws Exception {
    setup(v1Enabled, v2Enabled, systemMetricEnabled);
    validate(v1Enabled, v2Enabled, systemMetricEnabled);
    cleanup();
  }

  @Test
  public void testTimelineServiceV1V2Enabled() throws Exception {
    runTest(true, true, true);
  }

  @Test
  public void testTimelineServiceV1Enabled() throws Exception {
    runTest(true, false, true);
  }

  @Test
  public void testTimelineServiceV2Enabled() throws Exception {
    runTest(false, true, true);
  }

  @Test
  public void testTimelineServiceDisabled() throws Exception {
    runTest(false, false, true);
  }


  @Test
  public void testTimelineServiceV1V2EnabledSystemMetricDisable()
      throws Exception {
    runTest(true, true, false);
  }

  @Test
  public void testTimelineServiceV1EnabledSystemMetricDisable()
      throws Exception {
    runTest(true, false, false);
  }

  @Test
  public void testTimelineServiceV2EnabledSystemMetricDisable()
      throws Exception {
    runTest(false, true, false);
  }

  @Test
  public void testTimelineServiceDisabledSystemMetricDisable()
      throws Exception {
    runTest(false, false, false);
  }

}



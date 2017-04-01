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

package org.apache.hadoop.yarn.server.nodemanager.timelineservice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.client.api.impl.TimelineV2ClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.junit.Assert;
import org.junit.Test;

public class TestNMTimelinePublisher {
  private static final String MEMORY_ID = "MEMORY";
  private static final String CPU_ID = "CPU";

  @Test
  public void testContainerResourceUsage() {
    Context context = mock(Context.class);
    @SuppressWarnings("unchecked")
    final DummyTimelineClient timelineClient = new DummyTimelineClient(null);
    when(context.getNodeId()).thenReturn(NodeId.newInstance("localhost", 0));
    when(context.getHttpPort()).thenReturn(0);

    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);

    NMTimelinePublisher publisher = new NMTimelinePublisher(context) {
      public void createTimelineClient(ApplicationId appId) {
        if (!getAppToClientMap().containsKey(appId)) {
          timelineClient.init(getConfig());
          timelineClient.start();
          getAppToClientMap().put(appId, timelineClient);
        }
      }
    };
    publisher.init(conf);
    publisher.start();
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    publisher.createTimelineClient(appId);
    Container aContainer = mock(Container.class);
    when(aContainer.getContainerId()).thenReturn(ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(appId, 1),
        0L));
    publisher.reportContainerResourceUsage(aContainer, 1024L, 8F);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L, 8);
    timelineClient.reset();

    publisher.reportContainerResourceUsage(aContainer, 1024L, 0.8F);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L, 1);
    timelineClient.reset();

    publisher.reportContainerResourceUsage(aContainer, 1024L, 0.49F);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L, 0);
    timelineClient.reset();

    publisher.reportContainerResourceUsage(aContainer, 1024L,
        (float) ResourceCalculatorProcessTree.UNAVAILABLE);
    verifyPublishedResourceUsageMetrics(timelineClient, 1024L,
        ResourceCalculatorProcessTree.UNAVAILABLE);
    publisher.stop();
  }

  private void verifyPublishedResourceUsageMetrics(
      DummyTimelineClient timelineClient, long memoryUsage, int cpuUsage) {
    TimelineEntity[] entities = null;
    for (int i = 0; i < 10; i++) {
      entities = timelineClient.getLastPublishedEntities();
      if (entities != null) {
        break;
      }
      try {
        Thread.sleep(150L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    int numberOfResourceMetrics = 0;
    numberOfResourceMetrics +=
        (memoryUsage == ResourceCalculatorProcessTree.UNAVAILABLE) ? 0 : 1;
    numberOfResourceMetrics +=
        (cpuUsage == ResourceCalculatorProcessTree.UNAVAILABLE) ? 0 : 1;
    assertNotNull("entities are expected to be published", entities);
    assertEquals("Expected number of metrics notpublished",
        numberOfResourceMetrics, entities[0].getMetrics().size());
    Iterator<TimelineMetric> metrics = entities[0].getMetrics().iterator();
    while (metrics.hasNext()) {
      TimelineMetric metric = metrics.next();
      Iterator<Entry<Long, Number>> entrySet;
      switch (metric.getId()) {
      case CPU_ID:
        if (cpuUsage == ResourceCalculatorProcessTree.UNAVAILABLE) {
          Assert.fail("Not Expecting CPU Metric to be published");
        }
        entrySet = metric.getValues().entrySet().iterator();
        assertEquals("CPU usage metric not matching", cpuUsage,
            entrySet.next().getValue());
        break;
      case MEMORY_ID:
        if (memoryUsage == ResourceCalculatorProcessTree.UNAVAILABLE) {
          Assert.fail("Not Expecting Memory Metric to be published");
        }
        entrySet = metric.getValues().entrySet().iterator();
        assertEquals("Memory usage metric not matching", memoryUsage,
            entrySet.next().getValue());
        break;
      default:
        Assert.fail("Invalid Resource Usage metric");
        break;
      }
    }
  }

  protected static class DummyTimelineClient extends TimelineV2ClientImpl {
    public DummyTimelineClient(ApplicationId appId) {
      super(appId);
    }

    private TimelineEntity[] lastPublishedEntities;

    @Override
    public void putEntitiesAsync(TimelineEntity... entities)
        throws IOException, YarnException {
      this.lastPublishedEntities = entities;
    }

    public TimelineEntity[] getLastPublishedEntities() {
      return lastPublishedEntities;
    }

    public void reset() {
      lastPublishedEntities = null;
    }
  }
}

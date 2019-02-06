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

package org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.junit.Assert;
import org.junit.Test;

/**
 * Function test for {@link NMLogAggregationStatusTracker}.
 *
 */
public class TestNMLogAggregationStatusTracker {

  @SuppressWarnings("resource")
  @Test
  public void testNMLogAggregationStatusUpdate() {
    long baseTime = System.currentTimeMillis();
    Context mockContext = mock(Context.class);
    ConcurrentMap<ApplicationId, Application> apps = new ConcurrentHashMap<>();
    ApplicationId appId1 = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    apps.putIfAbsent(appId1, mock(Application.class));
    when(mockContext.getApplications()).thenReturn(apps);
    // the log aggregation is disabled.
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
    when(mockContext.getConf()).thenReturn(conf);
    NMLogAggregationStatusTracker tracker = new NMLogAggregationStatusTracker(
        mockContext);
    ApplicationId appId0 = ApplicationId.newInstance(0, 0);
    tracker.updateLogAggregationStatus(appId0,
        LogAggregationStatus.RUNNING, System.currentTimeMillis(), "", false);
    List<LogAggregationReport> reports = tracker
        .pullCachedLogAggregationReports();
    // we can not get any cached log aggregation status because
    // the log aggregation is disabled.
    Assert.assertTrue("No cached log aggregation status because "
        + "log aggregation is disabled.", reports.isEmpty());

    // enable the log aggregation.
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    when(mockContext.getConf()).thenReturn(conf);
    tracker = new NMLogAggregationStatusTracker(mockContext);
    // update the log aggregation status for an un-existed/finished
    // application, we should ignore the status update request.
    appId0 = ApplicationId.newInstance(0, 0);
    tracker.updateLogAggregationStatus(appId0,
        LogAggregationStatus.RUNNING, baseTime, "", false);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertTrue("No cached log aggregation status "
        + "because the application is finished or not existed.",
        reports.isEmpty());

    tracker.updateLogAggregationStatus(appId1,
        LogAggregationStatus.RUNNING, baseTime, "", false);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertEquals("Should have one cached log aggregation status.",
        1, reports.size());
    Assert.assertEquals("The cached log aggregation status should be RUNNING.",
        LogAggregationStatus.RUNNING,
        reports.get(0).getLogAggregationStatus());

    tracker.updateLogAggregationStatus(appId1,
        LogAggregationStatus.SUCCEEDED, baseTime + 60 * 1000, "", true);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertEquals(1, reports.size());
    Assert.assertEquals("Update cached log aggregation status to SUCCEEDED",
        LogAggregationStatus.SUCCEEDED,
        reports.get(0).getLogAggregationStatus());

    // the log aggregation status is finalized. So, we would
    // ingore the following update
    tracker.updateLogAggregationStatus(appId1,
        LogAggregationStatus.FAILED, baseTime + 10 * 60 * 1000, "", true);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertEquals(1, reports.size());
    Assert.assertEquals("The cached log aggregation status "
        + "should be still SUCCEEDED.", LogAggregationStatus.SUCCEEDED,
        reports.get(0).getLogAggregationStatus());
  }

  public void testLogAggregationStatusRoller() throws Exception {
    Context mockContext = mock(Context.class);
    Configuration conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
        10 * 1000);
    when(mockContext.getConf()).thenReturn(conf);
    ConcurrentMap<ApplicationId, Application> apps = new ConcurrentHashMap<>();
    ApplicationId appId1 = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    apps.putIfAbsent(appId1, mock(Application.class));
    when(mockContext.getApplications()).thenReturn(apps);
    final NMLogAggregationStatusTracker tracker =
        new NMLogAggregationStatusTracker(mockContext);
    tracker.updateLogAggregationStatus(appId1,
        LogAggregationStatus.RUNNING,
        System.currentTimeMillis(), "", false);
    // verify that we have cached the log aggregation status for app1
    List<LogAggregationReport> reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertEquals("Should have one cached log aggregation status.",
        1, reports.size());
    Assert.assertEquals("The cached log aggregation status should be RUNNING.",
        LogAggregationStatus.RUNNING,
        reports.get(0).getLogAggregationStatus());
    // wait for 10s
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        List<LogAggregationReport>reports = tracker
            .pullCachedLogAggregationReports();
        return reports.size() == 0;
      }
    }, 2000, 10000);
  }
}

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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.tracker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.junit.Assert;
import org.junit.Test;

public class TestNMLogAggregationStatusTracker {

  @Test
  public void testNMLogAggregationStatusUpdate() {
    Context mockContext = mock(Context.class);
    ConcurrentMap<ApplicationId, Application> apps = new ConcurrentHashMap<>();
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
    Assert.assertTrue(reports.isEmpty());

    // enable the log aggregation.
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    when(mockContext.getConf()).thenReturn(conf);
    tracker = new NMLogAggregationStatusTracker(mockContext);
    // update the log aggregation status for an un-existed application
    // the update time is not in the period of timeout.
    // So, we should not cache the log application status for this
    // application.
    appId0 = ApplicationId.newInstance(0, 0);
    tracker.updateLogAggregationStatus(appId0,
        LogAggregationStatus.RUNNING,
        System.currentTimeMillis() - 15 * 60 * 1000, "", false);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertTrue(reports.isEmpty());

    tracker.updateLogAggregationStatus(appId0,
        LogAggregationStatus.RUNNING,
        System.currentTimeMillis() - 60 * 1000, "", false);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertTrue(reports.size() == 1);
    Assert.assertTrue(reports.get(0).getLogAggregationStatus()
        == LogAggregationStatus.RUNNING);

    tracker.updateLogAggregationStatus(appId0,
        LogAggregationStatus.SUCCEEDED,
        System.currentTimeMillis() - 1 * 60 * 1000, "", true);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertTrue(reports.size() == 1);
    Assert.assertTrue(reports.get(0).getLogAggregationStatus()
        == LogAggregationStatus.SUCCEEDED);

    // the log aggregation status is finalized. So, we would
    // ingore the following update
    tracker.updateLogAggregationStatus(appId0,
        LogAggregationStatus.FAILED,
        System.currentTimeMillis() - 1 * 60 * 1000, "", true);
    reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertTrue(reports.size() == 1);
    Assert.assertTrue(reports.get(0).getLogAggregationStatus()
        == LogAggregationStatus.SUCCEEDED);
  }

  public void testLogAggregationStatusRoller() throws InterruptedException {
    Context mockContext = mock(Context.class);
    Configuration conf = new YarnConfiguration();
    conf.setLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
        10 * 1000);
    when(mockContext.getConf()).thenReturn(conf);
    NMLogAggregationStatusTracker tracker = new NMLogAggregationStatusTracker(
        mockContext);
    ApplicationId appId0 = ApplicationId.newInstance(0, 0);
    tracker.updateLogAggregationStatus(appId0,
        LogAggregationStatus.RUNNING,
        System.currentTimeMillis(), "", false);
    // sleep 10s
    Thread.sleep(10*1000);
    // the cache log aggregation status should be deleted.
    List<LogAggregationReport> reports = tracker
        .pullCachedLogAggregationReports();
    Assert.assertTrue(reports.size() == 0);
  }
}

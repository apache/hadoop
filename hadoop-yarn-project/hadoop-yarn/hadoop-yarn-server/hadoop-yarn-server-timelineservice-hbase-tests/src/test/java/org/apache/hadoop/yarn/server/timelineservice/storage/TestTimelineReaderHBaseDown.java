/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.timelineservice.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS;
import static org.apache.hadoop.yarn.server.timelineservice.storage.HBaseStorageMonitor.DATA_TO_RETRIEVE;
import static org.apache.hadoop.yarn.server.timelineservice.storage.HBaseStorageMonitor.MONITOR_FILTERS;

public class TestTimelineReaderHBaseDown {

  @Test(timeout=300000)
  public void testTimelineReaderHBaseUp() throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();
    configure(util);
    try {
      util.startMiniCluster();
      DataGeneratorForTest.createSchema(util.getConfiguration());
      DataGeneratorForTest.loadApps(util, System.currentTimeMillis());

      TimelineReaderServer server = getTimelineReaderServer();
      server.init(util.getConfiguration());
      HBaseTimelineReaderImpl htr = getHBaseTimelineReaderImpl(server);
      server.start();
      checkQuery(htr);
    } finally {
      util.shutdownMiniCluster();
    }
  }

  @Test(timeout=300000)
  public void testTimelineReaderInitWhenHBaseIsDown() throws
      TimeoutException, InterruptedException {
    HBaseTestingUtility util = new HBaseTestingUtility();
    configure(util);
    TimelineReaderServer server = getTimelineReaderServer();

    // init timeline reader when hbase is not running
    server.init(util.getConfiguration());
    HBaseTimelineReaderImpl htr = getHBaseTimelineReaderImpl(server);
    server.start();
    waitForHBaseDown(htr);
  }

  @Test(timeout=300000)
  public void testTimelineReaderDetectsHBaseDown() throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();
    configure(util);

    try {
      // start minicluster
      util.startMiniCluster();
      DataGeneratorForTest.createSchema(util.getConfiguration());
      DataGeneratorForTest.loadApps(util, System.currentTimeMillis());

      // init timeline reader
      TimelineReaderServer server = getTimelineReaderServer();
      server.init(util.getConfiguration());
      HBaseTimelineReaderImpl htr = getHBaseTimelineReaderImpl(server);

      // stop hbase after timeline reader init
      util.shutdownMiniHBaseCluster();

      // start server and check that it detects hbase is down
      server.start();
      waitForHBaseDown(htr);
    } finally {
      util.shutdownMiniCluster();
    }
  }

  @Test(timeout=300000)
  public void testTimelineReaderDetectsZooKeeperDown() throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();
    configure(util);

    try {
      // start minicluster
      util.startMiniCluster();
      DataGeneratorForTest.createSchema(util.getConfiguration());
      DataGeneratorForTest.loadApps(util, System.currentTimeMillis());

      // init timeline reader
      TimelineReaderServer server = getTimelineReaderServer();
      server.init(util.getConfiguration());
      HBaseTimelineReaderImpl htr = getHBaseTimelineReaderImpl(server);

      // stop hbase and zookeeper after timeline reader init
      util.shutdownMiniCluster();

      // start server and check that it detects hbase is down
      server.start();
      waitForHBaseDown(htr);
    } finally {
      util.shutdownMiniCluster();
    }
  }

  @Test(timeout=300000)
  public void testTimelineReaderRecoversAfterHBaseReturns() throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();
    configure(util);

    try {
      // start minicluster
      util.startMiniCluster();
      DataGeneratorForTest.createSchema(util.getConfiguration());
      DataGeneratorForTest.loadApps(util, System.currentTimeMillis());

      // init timeline reader
      TimelineReaderServer server = getTimelineReaderServer();
      server.init(util.getConfiguration());
      HBaseTimelineReaderImpl htr = getHBaseTimelineReaderImpl(server);

      // stop hbase after timeline reader init
      util.shutdownMiniHBaseCluster();

      // start server and check that it detects hbase is down
      server.start();
      waitForHBaseDown(htr);

      util.startMiniHBaseCluster(1, 1);
      GenericTestUtils.waitFor(() -> {
        try {
          htr.getTimelineStorageMonitor().checkStorageIsUp();
          return true;
        } catch (IOException e) {
          return false;
        }
      }, 1000, 150000);
    } finally {
      util.shutdownMiniCluster();
    }
  }

  private static void waitForHBaseDown(HBaseTimelineReaderImpl htr) throws
      TimeoutException, InterruptedException {
    try {
      GenericTestUtils.waitFor(() -> {
        try {
          htr.getTimelineStorageMonitor().checkStorageIsUp();
          return false;
        } catch (IOException e) {
          return true;
        }
      }, 1000, 150000);
      checkQuery(htr);
      Assert.fail("Query should fail when HBase is down");
    } catch (IOException e) {
      Assert.assertEquals("HBase is down", e.getMessage());
    }
  }

  private static Set<TimelineEntity> checkQuery(HBaseTimelineReaderImpl htr)
      throws IOException {
    TimelineReaderContext context =
        new TimelineReaderContext(YarnConfiguration.DEFAULT_RM_CLUSTER_ID,
            null, null, null, null, TimelineEntityType
            .YARN_FLOW_ACTIVITY.toString(), null, null);
    return htr.getEntities(context, MONITOR_FILTERS, DATA_TO_RETRIEVE);
  }

  private static void configure(HBaseTestingUtility util) {
    Configuration config = util.getConfiguration();
    config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    config.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
        "localhost:0");
    config.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
    config.set(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        "org.apache.hadoop.yarn.server.timelineservice.storage."
            + "HBaseTimelineReaderImpl");
    config.setInt("hfile.format.version", 3);
    config.setLong(TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS, 5000);
  }

  private static TimelineReaderServer getTimelineReaderServer() {
    return new TimelineReaderServer() {
      @Override
      protected void addFilters(Configuration conf) {
        // The parent code uses hadoop-common jar from this version of
        // Hadoop, but the tests are using hadoop-common jar from
        // ${hbase-compatible-hadoop.version}.  This version uses Jetty 9
        // while ${hbase-compatible-hadoop.version} uses Jetty 6, and there
        // are many differences, including classnames and packages.
        // We do nothing here, so that we don't cause a NoSuchMethodError or
        // NoClassDefFoundError.
        // Once ${hbase-compatible-hadoop.version} is changed to Hadoop 3,
        // we should be able to remove this @Override.
      }
    };
  }

  private static HBaseTimelineReaderImpl getHBaseTimelineReaderImpl(
      TimelineReaderServer server) {
    for (Service s: server.getServices()) {
      if (s instanceof HBaseTimelineReaderImpl) {
        return (HBaseTimelineReaderImpl) s;
      }
    }
    throw new IllegalStateException("Couldn't find HBaseTimelineReaderImpl");
  }
}

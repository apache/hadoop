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

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS;
import static org.junit.Assert.assertTrue;

/**
 * This class tests HbaseTimelineWriter with Hbase Down.
 */
public class TestTimelineWriterHBaseDown {

  @Test(timeout=300000)
  public void testTimelineWriterHBaseDown() throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();
    HBaseTimelineWriterImpl writer = new HBaseTimelineWriterImpl();
    try {
      Configuration c1 = util.getConfiguration();
      c1.setLong(TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS, 5000);
      writer.init(c1);
      writer.start();

      util.startMiniCluster();
      DataGeneratorForTest.createSchema(util.getConfiguration());

      TimelineStorageMonitor storageMonitor = writer.
          getTimelineStorageMonitor();
      waitForHBaseToUp(storageMonitor);

      try {
        storageMonitor.checkStorageIsUp();
      } catch(IOException e) {
        Assert.fail("HBaseStorageMonitor failed to detect HBase Up");
      }

      util.shutdownMiniHBaseCluster();
      waitForHBaseToDown(storageMonitor);

      TimelineEntities te = new TimelineEntities();
      ApplicationEntity entity = new ApplicationEntity();
      String appId = "application_1000178881110_2002";
      entity.setId(appId);
      Long cTime = 1425016501000L;
      entity.setCreatedTime(cTime);
      te.addEntity(entity);

      boolean exceptionCaught = false;
      try{
        writer.write(new TimelineCollectorContext("ATS1", "user1", "flow2",
            "AB7822C10F1111", 1002345678919L, appId), te,
            UserGroupInformation.createRemoteUser("user1"));
      } catch (IOException e) {
        if (e.getMessage().equals("HBase is down")) {
          exceptionCaught = true;
        }
      }
      assertTrue("HBaseStorageMonitor failed to detect HBase Down",
          exceptionCaught);
    } finally {
      writer.stop();
      util.shutdownMiniCluster();
    }
  }

  public void waitForHBaseToUp(TimelineStorageMonitor storageMonitor)
      throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        storageMonitor.checkStorageIsUp();
        return true;
      } catch (IOException e) {
        return false;
      }
    }, 1000, 150000);
  }

  public void waitForHBaseToDown(TimelineStorageMonitor storageMonitor)
      throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        storageMonitor.checkStorageIsUp();
        return false;
      } catch (IOException e) {
        return true;
      }
    }, 1000, 150000);
  }

}

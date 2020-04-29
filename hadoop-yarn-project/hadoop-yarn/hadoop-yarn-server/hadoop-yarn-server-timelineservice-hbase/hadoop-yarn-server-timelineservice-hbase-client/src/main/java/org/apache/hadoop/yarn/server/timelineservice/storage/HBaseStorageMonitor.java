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
package org.apache.hadoop.yarn.server.timelineservice.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReaderFactory;

/**
 * HBase based implementation for {@link TimelineStorageMonitor}.
 */
public class HBaseStorageMonitor extends TimelineStorageMonitor {

  protected static final TimelineEntityFilters MONITOR_FILTERS =
      new TimelineEntityFilters.Builder().entityLimit(1L).build();
  protected static final TimelineDataToRetrieve DATA_TO_RETRIEVE =
      new TimelineDataToRetrieve(null, null, null, null, null, null);

  private Configuration monitorHBaseConf;
  private Connection monitorConn;
  private TimelineEntityReader reader;

  public HBaseStorageMonitor(Configuration conf) throws Exception {
    super(conf, Storage.HBase);
    this.initialize(conf);
  }

  private void initialize(Configuration conf) throws  Exception {
    monitorHBaseConf = HBaseTimelineStorageUtils.
        getTimelineServiceHBaseConf(conf);
    monitorHBaseConf.setInt("hbase.client.retries.number", 3);
    monitorHBaseConf.setLong("hbase.client.pause", 1000);
    long monitorInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_STORAGE_MONITOR_INTERVAL_MS
    );
    monitorHBaseConf.setLong("hbase.rpc.timeout", monitorInterval);
    monitorHBaseConf.setLong("hbase.client.scanner.timeout.period",
        monitorInterval);
    monitorHBaseConf.setInt("zookeeper.recovery.retry", 1);
    monitorConn = ConnectionFactory.createConnection(monitorHBaseConf);

    String clusterId = conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    TimelineReaderContext monitorContext =
        new TimelineReaderContext(clusterId, null, null, null, null,
        TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), null, null);
    reader = TimelineEntityReaderFactory.createMultipleEntitiesReader(
        monitorContext, MONITOR_FILTERS, DATA_TO_RETRIEVE);
  }

  @Override
  public void healthCheck() throws Exception {
    reader.readEntities(monitorHBaseConf, monitorConn);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    monitorConn.close();
  }
}

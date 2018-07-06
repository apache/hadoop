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


import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.EntityTypeReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.server.timelineservice.storage.reader.TimelineEntityReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase based implementation for {@link TimelineReader}.
 */
public class HBaseTimelineReaderImpl
    extends AbstractService implements TimelineReader {

  private static final Logger LOG = LoggerFactory
      .getLogger(HBaseTimelineReaderImpl.class);

  private Configuration hbaseConf = null;
  private Connection conn;
  private Configuration monitorHBaseConf = null;
  private Connection monitorConn;
  private ScheduledExecutorService monitorExecutorService;
  private TimelineReaderContext monitorContext;
  private long monitorInterval;
  private AtomicBoolean hbaseDown = new AtomicBoolean();

  public HBaseTimelineReaderImpl() {
    super(HBaseTimelineReaderImpl.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);

    String clusterId = conf.get(
        YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
    monitorContext =
        new TimelineReaderContext(clusterId, null, null, null, null,
            TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), null, null);
    monitorInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_READER_STORAGE_MONITOR_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_STORAGE_MONITOR_INTERVAL_MS);

    monitorHBaseConf = HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(conf);
    monitorHBaseConf.setInt("hbase.client.retries.number", 3);
    monitorHBaseConf.setLong("hbase.client.pause", 1000);
    monitorHBaseConf.setLong("hbase.rpc.timeout", monitorInterval);
    monitorHBaseConf.setLong("hbase.client.scanner.timeout.period",
        monitorInterval);
    monitorHBaseConf.setInt("zookeeper.recovery.retry", 1);
    monitorConn = ConnectionFactory.createConnection(monitorHBaseConf);

    monitorExecutorService = Executors.newScheduledThreadPool(1);

    hbaseConf = HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(conf);
    conn = ConnectionFactory.createConnection(hbaseConf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    LOG.info("Scheduling HBase liveness monitor at interval {}",
        monitorInterval);
    monitorExecutorService.scheduleAtFixedRate(new HBaseMonitor(), 0,
        monitorInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (conn != null) {
      LOG.info("closing the hbase Connection");
      conn.close();
    }
    if (monitorExecutorService != null) {
      monitorExecutorService.shutdownNow();
      if (!monitorExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("failed to stop the monitir task in time. " +
            "will still proceed to close the monitor.");
      }
    }
    monitorConn.close();
    super.serviceStop();
  }

  private void checkHBaseDown() throws IOException {
    if (hbaseDown.get()) {
      throw new IOException("HBase is down");
    }
  }

  public boolean isHBaseDown() {
    return hbaseDown.get();
  }

  @Override
  public TimelineEntity getEntity(TimelineReaderContext context,
      TimelineDataToRetrieve dataToRetrieve) throws IOException {
    checkHBaseDown();
    TimelineEntityReader reader =
        TimelineEntityReaderFactory.createSingleEntityReader(context,
            dataToRetrieve);
    return reader.readEntity(hbaseConf, conn);
  }

  @Override
  public Set<TimelineEntity> getEntities(TimelineReaderContext context,
      TimelineEntityFilters filters, TimelineDataToRetrieve dataToRetrieve)
      throws IOException {
    checkHBaseDown();
    TimelineEntityReader reader =
        TimelineEntityReaderFactory.createMultipleEntitiesReader(context,
            filters, dataToRetrieve);
    return reader.readEntities(hbaseConf, conn);
  }

  @Override
  public Set<String> getEntityTypes(TimelineReaderContext context)
      throws IOException {
    checkHBaseDown();
    EntityTypeReader reader = new EntityTypeReader(context);
    return reader.readEntityTypes(hbaseConf, conn);
  }

  protected static final TimelineEntityFilters MONITOR_FILTERS =
      new TimelineEntityFilters.Builder().entityLimit(1L).build();
  protected static final TimelineDataToRetrieve DATA_TO_RETRIEVE =
      new TimelineDataToRetrieve(null, null, null, null, null, null);

  private class HBaseMonitor implements Runnable {
    @Override
    public void run() {
      try {
        LOG.info("Running HBase liveness monitor");
        TimelineEntityReader reader =
            TimelineEntityReaderFactory.createMultipleEntitiesReader(
                monitorContext, MONITOR_FILTERS, DATA_TO_RETRIEVE);
        reader.readEntities(monitorHBaseConf, monitorConn);

        // on success, reset hbase down flag
        if (hbaseDown.getAndSet(false)) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("HBase request succeeded, assuming HBase up");
          }
        }
      } catch (Exception e) {
        LOG.warn("Got failure attempting to read from timeline storage, " +
            "assuming HBase down", e);
        hbaseDown.getAndSet(true);
      }
    }
  }
}

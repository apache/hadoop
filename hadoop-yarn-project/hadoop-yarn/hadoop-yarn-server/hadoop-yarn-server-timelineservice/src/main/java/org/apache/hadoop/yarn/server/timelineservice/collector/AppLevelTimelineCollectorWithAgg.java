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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Service that handles aggregations for applications
 * and makes use of {@link AppLevelTimelineCollector} class for
 * writes to Timeline Service.
 *
 * App-related lifecycle management is handled by this service.
 */
@Private
@Unstable
public class AppLevelTimelineCollectorWithAgg
    extends AppLevelTimelineCollector {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollector.class);

  private final static int AGGREGATION_EXECUTOR_NUM_THREADS = 1;
  private int aggregationExecutorIntervalSecs;
  private static Set<String> entityTypesSkipAggregation
      = initializeSkipSet();

  private ScheduledThreadPoolExecutor appAggregationExecutor;
  private AppLevelAggregator appAggregator;

  public AppLevelTimelineCollectorWithAgg(ApplicationId appId, String user) {
    super(appId, user);
  }

  private static Set<String> initializeSkipSet() {
    Set<String> result = new HashSet<>();
    result.add(TimelineEntityType.YARN_APPLICATION.toString());
    result.add(TimelineEntityType.YARN_FLOW_RUN.toString());
    result.add(TimelineEntityType.YARN_FLOW_ACTIVITY.toString());
    return result;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    aggregationExecutorIntervalSecs = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_AGGREGATION_INTERVAL_SECS,
        YarnConfiguration.
            DEFAULT_TIMELINE_SERVICE_AGGREGATION_INTERVAL_SECS
    );
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // Launch the aggregation thread
    appAggregationExecutor = new ScheduledThreadPoolExecutor(
        AppLevelTimelineCollectorWithAgg.AGGREGATION_EXECUTOR_NUM_THREADS,
        new ThreadFactoryBuilder()
            .setNameFormat("TimelineCollector Aggregation thread #%d")
            .build());
    appAggregator = new AppLevelAggregator();
    appAggregationExecutor.scheduleAtFixedRate(appAggregator,
        aggregationExecutorIntervalSecs,
        aggregationExecutorIntervalSecs,
        TimeUnit.SECONDS);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    appAggregationExecutor.shutdown();
    if (!appAggregationExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
      LOG.info("App-level aggregator shutdown timed out, shutdown now. ");
      appAggregationExecutor.shutdownNow();
    }
    // Perform one round of aggregation after the aggregation executor is done.
    appAggregator.aggregate();
    super.serviceStop();
  }

  @Override
  protected Set<String> getEntityTypesSkipAggregation() {
    return entityTypesSkipAggregation;
  }

  private class AppLevelAggregator implements Runnable {

    private void aggregate() {
      LOG.debug("App-level real-time aggregating");
      if (!isReadyToAggregate()) {
        LOG.warn("App-level collector is not ready, skip aggregation. ");
        return;
      }
      try {
        TimelineCollectorContext currContext = getTimelineEntityContext();
        Map<String, AggregationStatusTable> aggregationGroups
            = getAggregationGroups();
        if (aggregationGroups == null
            || aggregationGroups.isEmpty()) {
          LOG.debug("App-level collector is empty, skip aggregation. ");
          return;
        }
        TimelineEntity resultEntity = TimelineCollector.aggregateWithoutGroupId(
            aggregationGroups, currContext.getAppId(),
            TimelineEntityType.YARN_APPLICATION.toString());
        TimelineEntities entities = new TimelineEntities();
        entities.addEntity(resultEntity);
        putEntitiesAsync(entities, getCurrentUser());
      } catch (Exception e) {
        LOG.error("Error aggregating timeline metrics", e);
      }
      LOG.debug("App-level real-time aggregation complete");
    }

    @Override
    public void run() {
      aggregate();
    }
  }

}

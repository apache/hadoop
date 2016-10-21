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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Service that handles writes to the timeline service and writes them to the
 * backing storage for a given YARN application.
 *
 * App-related lifecycle management is handled by this service.
 */
@Private
@Unstable
public class AppLevelTimelineCollector extends TimelineCollector {
  private static final Log LOG = LogFactory.getLog(TimelineCollector.class);

  private final static int AGGREGATION_EXECUTOR_NUM_THREADS = 1;
  private final static int AGGREGATION_EXECUTOR_EXEC_INTERVAL_SECS = 15;
  private static Set<String> entityTypesSkipAggregation
      = initializeSkipSet();

  private final ApplicationId appId;
  private final TimelineCollectorContext context;
  private ScheduledThreadPoolExecutor appAggregationExecutor;
  private AppLevelAggregator appAggregator;

  public AppLevelTimelineCollector(ApplicationId appId) {
    super(AppLevelTimelineCollector.class.getName() + " - " + appId.toString());
    Preconditions.checkNotNull(appId, "AppId shouldn't be null");
    this.appId = appId;
    context = new TimelineCollectorContext();
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
    context.setClusterId(conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID));
    // Set the default values, which will be updated with an RPC call to get the
    // context info from NM.
    // Current user usually is not the app user, but keep this field non-null
    context.setUserId(UserGroupInformation.getCurrentUser().getShortUserName());
    context.setAppId(appId.toString());
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // Launch the aggregation thread
    appAggregationExecutor = new ScheduledThreadPoolExecutor(
        AppLevelTimelineCollector.AGGREGATION_EXECUTOR_NUM_THREADS,
        new ThreadFactoryBuilder()
            .setNameFormat("TimelineCollector Aggregation thread #%d")
            .build());
    appAggregator = new AppLevelAggregator();
    appAggregationExecutor.scheduleAtFixedRate(appAggregator,
        AppLevelTimelineCollector.AGGREGATION_EXECUTOR_EXEC_INTERVAL_SECS,
        AppLevelTimelineCollector.AGGREGATION_EXECUTOR_EXEC_INTERVAL_SECS,
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
  public TimelineCollectorContext getTimelineEntityContext() {
    return context;
  }

  @Override
  protected Set<String> getEntityTypesSkipAggregation() {
    return entityTypesSkipAggregation;
  }

  private class AppLevelAggregator implements Runnable {

    private void aggregate() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("App-level real-time aggregating");
      }
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
        getWriter().write(currContext.getClusterId(), currContext.getUserId(),
            currContext.getFlowName(), currContext.getFlowVersion(),
            currContext.getFlowRunId(), currContext.getAppId(), entities);
      } catch (Exception e) {
        LOG.error("Error aggregating timeline metrics", e);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("App-level real-time aggregation complete");
      }
    }

    @Override
    public void run() {
      aggregate();
    }
  }

}

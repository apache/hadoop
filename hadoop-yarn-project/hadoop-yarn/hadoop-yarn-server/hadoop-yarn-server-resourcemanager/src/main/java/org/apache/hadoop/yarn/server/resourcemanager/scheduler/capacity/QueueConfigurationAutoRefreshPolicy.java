/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.io.IOException;


/**
 * Queue auto refresh policy for queues.
 */
public class QueueConfigurationAutoRefreshPolicy
    implements SchedulingEditPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueueConfigurationAutoRefreshPolicy.class);

  private Clock clock;

  // Pointer to other RM components
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;
  private RMNodeLabelsManager nlm;

  private long monitoringInterval;
  private long lastModified;

  // Last time we attempt to reload queues
  // included successful and failed case.
  private long lastReloadAttempt;
  private boolean lastReloadAttemptFailed = false;

  // Path to XML file containing allocations.
  private Path allocCsFile;
  private FileSystem fs;

  /**
   * Instantiated by CapacitySchedulerConfiguration.
   */
  public QueueConfigurationAutoRefreshPolicy() {
    clock = new MonotonicClock();
  }

  @Override
  public void init(final Configuration config, final RMContext context,
                   final ResourceScheduler sched) {
    LOG.info("Queue auto refresh Policy monitor: {}" + this.
        getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    clock = scheduler.getClock();

    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();

    CapacitySchedulerConfiguration csConfig = scheduler.getConfiguration();

    monitoringInterval = csConfig.getLong(
        CapacitySchedulerConfiguration.QUEUE_AUTO_REFRESH_MONITORING_INTERVAL,
        CapacitySchedulerConfiguration.
            DEFAULT_QUEUE_AUTO_REFRESH_MONITORING_INTERVAL);
  }


  @Override
  public void editSchedule() {
    long startTs = clock.getTime();

    try {

      // Support both FileSystemBased and LocalFile based
      if (rmContext.getYarnConfiguration().
          get(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS).
          equals(FileSystemBasedConfigurationProvider
              .class.getCanonicalName())) {
        allocCsFile = new Path(rmContext.getYarnConfiguration().
            get(YarnConfiguration.FS_BASED_RM_CONF_STORE),
            YarnConfiguration.CS_CONFIGURATION_FILE);
      } else {
        allocCsFile =  new Path(rmContext.getYarnConfiguration()
            .getClassLoader().getResource("").toString(),
            YarnConfiguration.CS_CONFIGURATION_FILE);
      }

      // Check if the cs related conf modified
      fs =  allocCsFile.getFileSystem(rmContext.getYarnConfiguration());

      lastModified =
          fs.getFileStatus(allocCsFile).getModificationTime();

      long time = clock.getTime();

      if (lastModified > lastReloadAttempt &&
          time > lastReloadAttempt + monitoringInterval) {
        try {
          rmContext.getRMAdminService().refreshQueues();
          LOG.info("Queue auto refresh completed successfully");
          lastReloadAttempt = clock.getTime();
        } catch (IOException | YarnException e) {
          LOG.error("Can't refresh queue: " + e);
          if (!lastReloadAttemptFailed) {
            LOG.error("Failed to reload capacity scheduler config file - " +
                "will use existing conf. Message: {}", e.getMessage());
          }
          lastReloadAttempt = clock.getTime();
          lastReloadAttemptFailed = true;
        }

      } else if (lastModified == 0L) {
        if (!lastReloadAttemptFailed) {
          LOG.warn("Failed to reload capacity scheduler config file because" +
              " last modified returned 0. File exists: "
              + fs.exists(allocCsFile));
        }
        lastReloadAttemptFailed = true;
      }

    } catch (IOException e) {
      LOG.error("Can't get file status for refresh : " + e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  @VisibleForTesting
  long getLastReloadAttempt() {
    return lastReloadAttempt;
  }

  @VisibleForTesting
  long getLastModified() {
    return lastModified;
  }

  @VisibleForTesting
  Clock getClock() {
    return clock;
  }

  @VisibleForTesting
  boolean getLastReloadAttemptFailed() {
    return  lastReloadAttemptFailed;
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return QueueConfigurationAutoRefreshPolicy.class.getCanonicalName();
  }
}

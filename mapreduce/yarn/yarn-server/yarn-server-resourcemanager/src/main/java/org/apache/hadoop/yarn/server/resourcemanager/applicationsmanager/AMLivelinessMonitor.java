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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEventType;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * This class runs continuosly to track the application masters
 * that might be dead.
 */
public class AMLivelinessMonitor extends AbstractService {
  private volatile boolean stop = false;
  long monitoringInterval =
      RMConfig.DEFAULT_AMLIVELINESS_MONITORING_INTERVAL;
  private final EventHandler handler;
  private final Clock clock;
  private long amExpiryInterval;

  private static final Log LOG = LogFactory
      .getLog(AMLivelinessMonitor.class);

  private static final class AMLifeStatus {
    ApplicationId applicationId;
    long lastSeen;

    public AMLifeStatus(ApplicationId appId, long lastSeen) {
      this.applicationId = appId;
      this.lastSeen = lastSeen;
    }
  }

  private static final Comparator<AMLifeStatus> APPLICATION_STATUS_COMPARATOR 
    = new Comparator<AMLifeStatus>() {
        public int compare(AMLifeStatus s1, AMLifeStatus s2) {
          if (s1.lastSeen < s2.lastSeen) {
            return -1;
          } else if (s1.lastSeen > s2.lastSeen) {
            return 1;
          } else {
            // TODO: What if cluster-time-stamp changes on RM restart.
            return (s1.applicationId.getId() - s2.applicationId.getId());
          }
        }
  };

  private Map<ApplicationId, AMLifeStatus> apps
    = new HashMap<ApplicationId, AMLifeStatus>();

  private TreeSet<AMLifeStatus> amExpiryQueue =
    new TreeSet<AMLifeStatus>(APPLICATION_STATUS_COMPARATOR);

  public AMLivelinessMonitor(EventHandler handler) {
    super("ApplicationsManager:" + AMLivelinessMonitor.class.getName());
    this.handler = handler;
    this.clock = new SystemClock();
  }

  @Override
  public synchronized void init(Configuration conf) {
    this.amExpiryInterval = conf.getLong(YarnConfiguration.AM_EXPIRY_INTERVAL,
        RMConfig.DEFAULT_AM_EXPIRY_INTERVAL);
    LOG.info("AM expiry interval: " + this.amExpiryInterval);
    this.monitoringInterval = conf.getLong(
        RMConfig.AMLIVELINESS_MONITORING_INTERVAL,
        RMConfig.DEFAULT_AMLIVELINESS_MONITORING_INTERVAL);
    super.init(conf);
  }

  private final Thread monitoringThread = new Thread() {
    @Override
    public void run() {

      /*
       * the expiry queue does not need to be in sync with applications, if an
       * applications in the expiry queue cannot be found in applications its
       * alright. We do not want to hold a lock on applications while going
       * through the expiry queue.
       */
      List<ApplicationId> expired = new ArrayList<ApplicationId>();
      while (!stop) {
        AMLifeStatus leastRecent;
        long now = System.currentTimeMillis();
        expired.clear();
        synchronized (amExpiryQueue) {
          while ((amExpiryQueue.size() > 0)
              && (leastRecent = amExpiryQueue.first()) != null
              && ((now - leastRecent.lastSeen) > amExpiryInterval)) {
            amExpiryQueue.remove(leastRecent);
            if ((now - leastRecent.lastSeen) > amExpiryInterval) {
              expired.add(leastRecent.applicationId);
            } else {
              amExpiryQueue.add(leastRecent);
            }
          }
        }
        expireAMs(expired);
        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }
  };

  @Override
  public synchronized void start() {
    this.monitoringThread.start();
    super.start();
  }

  public void stop() {
    stop = true;
    try {
      this.monitoringThread.join();
    } catch (InterruptedException e) {
      ;
    }
    super.stop();
  }

  private void expireAMs(List<ApplicationId> toExpire) {
    for (ApplicationId applicationId: toExpire) {
      LOG.info("Expiring the Application " + applicationId);
      handler.handle(new ApplicationEvent(
          ApplicationEventType.EXPIRE, applicationId));
    }
  }

  void register(ApplicationId appId) {
    LOG.info("Adding application master for tracking " + appId);
    synchronized (this.amExpiryQueue) {
      AMLifeStatus status = new AMLifeStatus(appId, this.clock.getTime());
      this.amExpiryQueue.add(status);
      this.apps.put(appId, status);
    }
  }

  void receivedPing(ApplicationId applicationId) {
    synchronized (this.amExpiryQueue) {
      AMLifeStatus status = this.apps.get(applicationId);
      if (this.amExpiryQueue.remove(status)) {
        status.lastSeen = this.clock.getTime();
        this.amExpiryQueue.add(status);
      }
    }
  }

  void unRegister(ApplicationId appId) {
    LOG.info("Removing application master from tracking: " + appId);
    synchronized (this.amExpiryQueue) {
      AMLifeStatus status = this.apps.get(appId);
      this.amExpiryQueue.remove(status);
    }
  }
}
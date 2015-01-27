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

package org.apache.hadoop.yarn.server.timelineservice.aggregator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Class that manages adding and removing app level aggregator services and
 * their lifecycle. It provides thread safety access to the app level services.
 *
 * It is a singleton, and instances should be obtained via
 * {@link #getInstance()}.
 */
@Private
@Unstable
public class AppLevelServiceManager extends CompositeService {
  private static final Log LOG =
      LogFactory.getLog(AppLevelServiceManager.class);
  private static final AppLevelServiceManager INSTANCE =
      new AppLevelServiceManager();

  // access to this map is synchronized with the map itself
  private final Map<String,AppLevelAggregatorService> services =
      Collections.synchronizedMap(
          new HashMap<String,AppLevelAggregatorService>());

  static AppLevelServiceManager getInstance() {
    return INSTANCE;
  }

  AppLevelServiceManager() {
    super(AppLevelServiceManager.class.getName());
  }

  /**
   * Creates and adds an app level aggregator service for the specified
   * application id. The service is also initialized and started. If the service
   * already exists, no new service is created.
   *
   * @throws YarnRuntimeException if there was any exception in initializing and
   * starting the app level service
   * @return whether it was added successfully
   */
  public boolean addService(String appId) {
    synchronized (services) {
      AppLevelAggregatorService service = services.get(appId);
      if (service == null) {
        try {
          service = new AppLevelAggregatorService(appId);
          // initialize, start, and add it to the parent service so it can be
          // cleaned up when the parent shuts down
          service.init(getConfig());
          service.start();
          services.put(appId, service);
          LOG.info("the application aggregator service for " + appId +
              " was added");
          return true;
        } catch (Exception e) {
          throw new YarnRuntimeException(e);
        }
      } else {
        String msg = "the application aggregator service for " + appId +
            " already exists!";
        LOG.error(msg);
        return false;
      }
    }
  }

  /**
   * Removes the app level aggregator service for the specified application id.
   * The service is also stopped as a result. If the service does not exist, no
   * change is made.
   *
   * @return whether it was removed successfully
   */
  public boolean removeService(String appId) {
    synchronized (services) {
      AppLevelAggregatorService service = services.remove(appId);
      if (service == null) {
        String msg = "the application aggregator service for " + appId +
            " does not exist!";
        LOG.error(msg);
        return false;
      } else {
        // stop the service to do clean up
        service.stop();
        LOG.info("the application aggregator service for " + appId +
            " was removed");
        return true;
      }
    }
  }

  /**
   * Returns the app level aggregator service for the specified application id.
   *
   * @return the app level aggregator service or null if it does not exist
   */
  public AppLevelAggregatorService getService(String appId) {
    return services.get(appId);
  }

  /**
   * Returns whether the app level aggregator service for the specified
   * application id exists.
   */
  public boolean hasService(String appId) {
    return services.containsKey(appId);
  }
}

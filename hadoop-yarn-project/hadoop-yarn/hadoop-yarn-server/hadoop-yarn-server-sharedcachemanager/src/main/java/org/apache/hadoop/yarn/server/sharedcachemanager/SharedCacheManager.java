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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.webapp.SCMWebServer;

import com.google.common.annotations.VisibleForTesting;

/**
 * This service maintains the shared cache meta data. It handles claiming and
 * releasing of resources, all rpc calls from the client to the shared cache
 * manager, and administrative commands. It also persists the shared cache meta
 * data to a backend store, and cleans up stale entries on a regular basis.
 */
@Private
@Unstable
public class SharedCacheManager extends CompositeService {
  /**
   * Priority of the SharedCacheManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(SharedCacheManager.class);

  private SCMStore store;

  public SharedCacheManager() {
    super("SharedCacheManager");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.store = createSCMStoreService(conf);
    addService(store);

    CleanerService cs = createCleanerService(store);
    addService(cs);

    SharedCacheUploaderService nms =
        createNMCacheUploaderSCMProtocolService(store);
    addService(nms);

    ClientProtocolService cps = createClientProtocolService(store);
    addService(cps);

    SCMAdminProtocolService saps = createSCMAdminProtocolService(cs);
    addService(saps);

    SCMWebServer webUI = createSCMWebServer(this);
    addService(webUI);

    // init metrics
    DefaultMetricsSystem.initialize("SharedCacheManager");
    JvmMetrics.initSingleton("SharedCacheManager", null);

    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  private static SCMStore createSCMStoreService(Configuration conf) {
    Class<? extends SCMStore> defaultStoreClass;
    try {
      defaultStoreClass =
          (Class<? extends SCMStore>) Class
              .forName(YarnConfiguration.DEFAULT_SCM_STORE_CLASS);
    } catch (Exception e) {
      throw new YarnRuntimeException("Invalid default scm store class"
          + YarnConfiguration.DEFAULT_SCM_STORE_CLASS, e);
    }

    SCMStore store =
        ReflectionUtils.newInstance(conf.getClass(
            YarnConfiguration.SCM_STORE_CLASS,
            defaultStoreClass, SCMStore.class), conf);
    return store;
  }

  private CleanerService createCleanerService(SCMStore store) {
    return new CleanerService(store);
  }

  private SharedCacheUploaderService
      createNMCacheUploaderSCMProtocolService(SCMStore store) {
    return new SharedCacheUploaderService(store);
  }

  private ClientProtocolService createClientProtocolService(SCMStore store) {
    return new ClientProtocolService(store);
  }

  private SCMAdminProtocolService createSCMAdminProtocolService(
      CleanerService cleanerService) {
    return new SCMAdminProtocolService(cleanerService);
  }

  private SCMWebServer createSCMWebServer(SharedCacheManager scm) {
    return new SCMWebServer(scm);
  }

  @Override
  protected void serviceStop() throws Exception {

    DefaultMetricsSystem.shutdown();
    super.serviceStop();
  }

  /**
   * For testing purposes only.
   */
  @VisibleForTesting
  SCMStore getSCMStore() {
    return this.store;
  }

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(SharedCacheManager.class, args, LOG);
    try {
      Configuration conf = new YarnConfiguration();
      SharedCacheManager sharedCacheManager = new SharedCacheManager();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(sharedCacheManager),
          SHUTDOWN_HOOK_PRIORITY);
      sharedCacheManager.init(conf);
      sharedCacheManager.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting SharedCacheManager", t);
      System.exit(-1);
    }
  }
}

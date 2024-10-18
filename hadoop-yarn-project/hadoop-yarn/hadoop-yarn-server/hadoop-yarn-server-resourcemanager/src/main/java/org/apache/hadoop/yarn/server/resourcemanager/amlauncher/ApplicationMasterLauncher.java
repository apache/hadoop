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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;


public class ApplicationMasterLauncher extends AbstractService implements
    EventHandler<AMLauncherEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ApplicationMasterLauncher.class);
  private ThreadPoolExecutor launcherPool;
  private ThreadPoolExecutor cleanupPool;
  private LauncherThread launcherHandlingThread;
  private CleanupThread cleanupHandlingThread;

  
  private final BlockingQueue<Runnable> launcherEvents
    = new LinkedBlockingQueue<Runnable>();
  private final BlockingQueue<Runnable> cleanupEvents
    = new LinkedBlockingQueue<Runnable>();
  protected final RMContext context;
  
  public ApplicationMasterLauncher(RMContext context) {
    super(ApplicationMasterLauncher.class.getName());
    this.context = context;
    this.launcherHandlingThread = new LauncherThread();
    this.cleanupHandlingThread = new CleanupThread();
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    int launcherThreadCount = conf.getInt(
        YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);
    ThreadFactory tfLauncher = new ThreadFactoryBuilder()
        .setNameFormat("ApplicationMasterLauncher #%d")
        .build();
    launcherPool = new ThreadPoolExecutor(launcherThreadCount, launcherThreadCount, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
    launcherPool.setThreadFactory(tfLauncher);

    int cleanupThreadCount = conf.getInt(
            YarnConfiguration.RM_AMCLEANUP_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_AMCLEANUP_THREAD_COUNT);
    ThreadFactory tfCleanup = new ThreadFactoryBuilder()
            .setNameFormat("ApplicationMasterLauncher #%d")
            .build();
    cleanupPool = new ThreadPoolExecutor(cleanupThreadCount, cleanupThreadCount, 1,
            TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
    cleanupPool.setThreadFactory(tfCleanup);

    Configuration newConf = new YarnConfiguration(conf);
    newConf.setInt(CommonConfigurationKeysPublic.
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
            YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES));
    setConfig(newConf);
    super.serviceInit(newConf);
  }

  @Override
  protected void serviceStart() throws Exception {
    launcherHandlingThread.start();
    cleanupHandlingThread.start();
    super.serviceStart();
  }
  
  protected Runnable createRunnableLauncher(RMAppAttempt application, 
      AMLauncherEventType event) {
    Runnable launcher =
        new AMLauncher(context, application, event, getConfig());
    return launcher;
  }
  
  private void launch(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, 
        AMLauncherEventType.LAUNCH);
    launcherEvents.add(launcher);
  }
  

  @Override
  protected void serviceStop() throws Exception {
    launcherHandlingThread.interrupt();
    cleanupHandlingThread.interrupt();

    try {
      launcherHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info(launcherHandlingThread.getName()
              + " interrupted during join ", ie);
    }

    try {
      cleanupHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info(cleanupHandlingThread.getName()
              + " interrupted during join ", ie);
    }

    launcherPool.shutdown();
    cleanupPool.shutdown();
  }

  private class LauncherThread extends Thread {
    
    public LauncherThread() {
      super("ApplicationMaster Launcher");
    }

    @Override
    public void run() {
      while (!this.isInterrupted()) {
        Runnable toLaunch;

        try {
          toLaunch = launcherEvents.take();
          launcherPool.execute(toLaunch);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }
  }

  private class CleanupThread extends Thread {

    public CleanupThread() {
      super("ApplicationMaster Cleanup");
    }

    @Override
    public void run() {
      while (!this.isInterrupted()) {
        Runnable toCleanup;

        try {
          toCleanup = cleanupEvents.take();
          cleanupPool.execute(toCleanup);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }
  }

  private void cleanup(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
    cleanupEvents.add(launcher);
  } 
  
  @Override
  public synchronized void  handle(AMLauncherEvent appEvent) {
    AMLauncherEventType event = appEvent.getType();
    RMAppAttempt application = appEvent.getAppAttempt();
    switch (event) {
    case LAUNCH:
      launch(application);
      break;
    case CLEANUP:
      cleanup(application);
      break;
    default:
      break;
    }
  }
}

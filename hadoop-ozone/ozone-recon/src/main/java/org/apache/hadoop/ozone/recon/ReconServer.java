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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INTERVAL;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;


/**
 * Recon server main class that stops and starts recon services.
 */
public class ReconServer extends GenericCli {

  private static final Logger LOG = LoggerFactory.getLogger(ReconServer.class);
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1);
  private Injector injector;

  @Inject
  private ReconHttpServer httpServer;

  public static void main(String[] args) {
    new ReconServer().run(args);
  }

  @Override
  public Void call() throws Exception {
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    OzoneConfigurationProvider.setConfiguration(ozoneConfiguration);

    injector =  Guice.createInjector(new
        ReconControllerModule(), new ReconRestServletModule() {
          @Override
          protected void configureServlets() {
            rest("/api/*")
                .packages("org.apache.hadoop.ozone.recon.api");
          }
        });

    //Pass on injector to listener that does the Guice - Jersey HK2 bridging.
    ReconGuiceServletContextListener.setInjector(injector);

    httpServer = injector.getInstance(ReconHttpServer.class);
    LOG.info("Starting Recon server");
    httpServer.start();
    scheduleReconTasks();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        stop();
      } catch (Exception e) {
        LOG.error("Error during stop Recon server", e);
      }
    }));
    return null;
  }

  /**
   * Schedule the tasks that is required by Recon to keep its metadata up to
   * date.
   */
  private void scheduleReconTasks() {
    OzoneConfiguration configuration = injector.getInstance(
        OzoneConfiguration.class);
    ContainerDBServiceProvider containerDBServiceProvider = injector
        .getInstance(ContainerDBServiceProvider.class);
    OzoneManagerServiceProvider ozoneManagerServiceProvider = injector
        .getInstance(OzoneManagerServiceProvider.class);

    // Schedule the task to read OM DB and write the reverse mapping to Recon
    // container DB.
    ContainerKeyMapperTask containerKeyMapperTask = new ContainerKeyMapperTask(
        ozoneManagerServiceProvider, containerDBServiceProvider);
    long initialDelay = configuration.getTimeDuration(
        RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
        RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT,
        TimeUnit.MILLISECONDS);
    long interval = configuration.getTimeDuration(
        RECON_OM_SNAPSHOT_TASK_INTERVAL,
        RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    scheduler.scheduleWithFixedDelay(containerKeyMapperTask, initialDelay,
        interval, TimeUnit.MILLISECONDS);
  }

  void stop() throws Exception {
    LOG.info("Stopping Recon server");
    httpServer.stop();
  }
}

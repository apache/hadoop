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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.service.client.params.SliderAMArgs;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ServiceMaster extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceMaster.class);

  private static SliderAMArgs amArgs;
  protected ServiceContext context;

  public ServiceMaster(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    //TODO Deprecate slider conf, make sure works with yarn conf
    printSystemEnv();
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation.setConfiguration(conf);
    }
    LOG.info("Login user is {}", UserGroupInformation.getLoginUser());

    context = new ServiceContext();
    Path appDir = getAppDir();
    SliderFileSystem fs = new SliderFileSystem(conf);
    context.fs = fs;
    fs.setAppDir(appDir);
    loadApplicationJson(context, fs);

    ContainerId amContainerId = getAMContainerId();

    ApplicationAttemptId attemptId = amContainerId.getApplicationAttemptId();
    LOG.info("Application attemptId: " + attemptId);
    context.attemptId = attemptId;

    // configure AM to wait forever for RM
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, -1);
    conf.unset(YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS);

    DefaultMetricsSystem.initialize("ServiceAppMaster");

    context.secretManager = new ClientToAMTokenSecretManager(attemptId, null);
    ClientAMService clientAMService = new ClientAMService(context);
    context.clientAMService = clientAMService;
    addService(clientAMService);

    ServiceScheduler scheduler = createServiceScheduler(context);
    addService(scheduler);
    context.scheduler = scheduler;

    ServiceMonitor monitor = new ServiceMonitor("Service Monitor", context);
    addService(monitor);

    super.serviceInit(conf);
  }

  protected ContainerId getAMContainerId() throws BadClusterStateException {
    return ContainerId.fromString(SliderUtils.mandatoryEnvVariable(
        ApplicationConstants.Environment.CONTAINER_ID.name()));
  }

  protected Path getAppDir() {
    return new Path(amArgs.getAppDefPath()).getParent();
  }

  protected ServiceScheduler createServiceScheduler(ServiceContext context)
      throws IOException, YarnException {
    return new ServiceScheduler(context);
  }

  protected void loadApplicationJson(ServiceContext context,
      SliderFileSystem fs) throws IOException {
    context.application = ServiceApiUtil
        .loadApplicationFrom(fs, new Path(amArgs.getAppDefPath()));
    LOG.info(context.application.toString());
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping app master");
    super.serviceStop();
  }

  private void printSystemEnv() {
    for (Map.Entry<String, String> envs : System.getenv().entrySet()) {
      LOG.info("{} = {}", envs.getKey(), envs.getValue());
    }
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ServiceMaster.class, args, LOG);
    amArgs = new SliderAMArgs(args);
    amArgs.parse();
    try {
      ServiceMaster serviceMaster = new ServiceMaster("Service Master");
      ShutdownHookManager.get()
          .addShutdownHook(new CompositeServiceShutdownHook(serviceMaster), 30);
      YarnConfiguration conf = new YarnConfiguration();
      new GenericOptionsParser(conf, args);
      serviceMaster.init(conf);
      serviceMaster.start();
    } catch (Throwable t) {
      LOG.error("Error starting service master", t);
      ExitUtil.terminate(1, "Error starting service master");
    }
  }
}

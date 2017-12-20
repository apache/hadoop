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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.exceptions.BadClusterStateException;
import org.apache.hadoop.yarn.service.monitor.ServiceMonitor;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.yarn.service.conf.YarnServiceConstants.KEYTAB_LOCATION;

public class ServiceMaster extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceMaster.class);

  public static final String YARNFILE_OPTION = "yarnfile";

  private static String serviceDefPath;
  protected ServiceContext context;

  public ServiceMaster(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    printSystemEnv();
    context = new ServiceContext();
    Path appDir = getAppDir();
    context.serviceHdfsDir = appDir.toString();
    SliderFileSystem fs = new SliderFileSystem(conf);
    context.fs = fs;
    fs.setAppDir(appDir);
    loadApplicationJson(context, fs);

    if (UserGroupInformation.isSecurityEnabled()) {
      context.tokens = recordTokensForContainers();
      doSecureLogin();
    }
    // Take yarn config from YarnFile and merge them into YarnConfiguration
    for (Map.Entry<String, String> entry : context.service
        .getConfiguration().getProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    ContainerId amContainerId = getAMContainerId();

    ApplicationAttemptId attemptId = amContainerId.getApplicationAttemptId();
    LOG.info("Service AppAttemptId: " + attemptId);
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

  // Record the tokens and use them for launching containers.
  // e.g. localization requires the hdfs delegation tokens
  private ByteBuffer recordTokensForContainers() throws IOException {
    Credentials copy = new Credentials(UserGroupInformation.getCurrentUser()
        .getCredentials());
    DataOutputBuffer dob = new DataOutputBuffer();
    try {
      copy.writeTokenStorageToStream(dob);
    } finally {
      dob.close();
    }
    // Now remove the AM->RM token so that task containers cannot access it.
    Iterator<Token<?>> iter = copy.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token.toString());
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
  }

  // 1. First try to use user specified keytabs
  // 2. If not specified, then try to use pre-installed keytab at localhost
  // 3. strip off hdfs delegation tokens to ensure use keytab to talk to hdfs
  private void doSecureLogin()
      throws IOException, URISyntaxException {
    // read the localized keytab specified by user
    File keytab = new File(String.format(KEYTAB_LOCATION,
        context.service.getName()));
    if (!keytab.exists()) {
      LOG.info("No keytab localized at " + keytab);
      // Check if there exists a pre-installed keytab at host
      String preInstalledKeytab = context.service.getKerberosPrincipal()
          .getKeytab();
      if (!StringUtils.isEmpty(preInstalledKeytab)) {
        URI uri = new URI(preInstalledKeytab);
        if (uri.getScheme().equals("file")) {
          keytab = new File(uri);
          LOG.info("Using pre-installed keytab from localhost: " +
              preInstalledKeytab);
        }
      }
    }
    if (!keytab.exists()) {
      LOG.info("No keytab exists: " + keytab);
      return;
    }
    String principal = context.service.getKerberosPrincipal()
        .getPrincipalName();
    if (StringUtils.isEmpty((principal))) {
      principal = UserGroupInformation.getLoginUser().getShortUserName();
      LOG.info("No principal name specified.  Will use AM " +
          "login identity {} to attempt keytab-based login", principal);
    }

    Credentials credentials = UserGroupInformation.getCurrentUser()
        .getCredentials();
    LOG.info("User before logged in is: " + UserGroupInformation
        .getCurrentUser());
    String principalName = SecurityUtil.getServerPrincipal(principal,
        ServiceUtils.getLocalHostName(getConfig()));
    UserGroupInformation.loginUserFromKeytab(principalName,
        keytab.getAbsolutePath());
    // add back the credentials
    UserGroupInformation.getCurrentUser().addCredentials(credentials);
    LOG.info("User after logged in is: " + UserGroupInformation
        .getCurrentUser());
    context.principal = principalName;
    context.keytab = keytab.getAbsolutePath();
    removeHdfsDelegationToken(UserGroupInformation.getLoginUser());
  }

  // Remove HDFS delegation token from login user and ensure AM to use keytab
  // to talk to hdfs
  private static void removeHdfsDelegationToken(UserGroupInformation user) {
    if (!user.isFromKeytab()) {
      LOG.error("AM is not holding on a keytab in a secure deployment:" +
          " service will fail when tokens expire");
    }
    Credentials credentials = user.getCredentials();
    Iterator<Token<? extends TokenIdentifier>> iter =
        credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<? extends TokenIdentifier> token = iter.next();
      if (token.getKind().equals(
          DelegationTokenIdentifier.HDFS_DELEGATION_KIND)) {
        LOG.info("Remove HDFS delegation token {}.", token);
        iter.remove();
      }
    }
  }

  protected ContainerId getAMContainerId() throws BadClusterStateException {
    return ContainerId.fromString(ServiceUtils.mandatoryEnvVariable(
        ApplicationConstants.Environment.CONTAINER_ID.name()));
  }

  protected Path getAppDir() {
    return new Path(serviceDefPath).getParent();
  }

  protected ServiceScheduler createServiceScheduler(ServiceContext context)
      throws IOException, YarnException {
    return new ServiceScheduler(context);
  }

  protected void loadApplicationJson(ServiceContext context,
      SliderFileSystem fs) throws IOException {
    context.service = ServiceApiUtil
        .loadServiceFrom(fs, new Path(serviceDefPath));
    context.service.setState(ServiceState.ACCEPTED);
    LOG.info(context.service.toString());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting service as user " + UserGroupInformation
        .getCurrentUser());
    UserGroupInformation.getLoginUser().doAs(
        (PrivilegedExceptionAction<Void>) () -> {
          super.serviceStart();
          return null;
        }
    );
  }
  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping app master");
    super.serviceStop();
  }

  // This method should be called whenever there is an increment or decrement
  // of a READY state component of a service
  public static synchronized void checkAndUpdateServiceState(
      ServiceScheduler scheduler, boolean isIncrement) {
    ServiceState curState = scheduler.getApp().getState();
    if (!isIncrement) {
      // set it to STARTED every time a component moves out of STABLE state
      scheduler.getApp().setState(ServiceState.STARTED);
    } else {
      // otherwise check the state of all components
      boolean isStable = true;
      for (org.apache.hadoop.yarn.service.api.records.Component comp : scheduler
          .getApp().getComponents()) {
        if (comp.getState() !=
            org.apache.hadoop.yarn.service.api.records.ComponentState.STABLE) {
          isStable = false;
          break;
        }
      }
      if (isStable) {
        scheduler.getApp().setState(ServiceState.STABLE);
      } else {
        // mark new state as started only if current state is stable, otherwise
        // leave it as is
        if (curState == ServiceState.STABLE) {
          scheduler.getApp().setState(ServiceState.STARTED);
        }
      }
    }
    if (curState != scheduler.getApp().getState()) {
      LOG.info("Service state changed from {} -> {}", curState,
          scheduler.getApp().getState());
    }
  }

  private void printSystemEnv() {
    for (Map.Entry<String, String> envs : System.getenv().entrySet()) {
      LOG.info("{} = {}", envs.getKey(), envs.getValue());
    }
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    org.apache.hadoop.util.StringUtils
        .startupShutdownMessage(ServiceMaster.class, args, LOG);
    try {
      ServiceMaster serviceMaster = new ServiceMaster("Service Master");
      ShutdownHookManager.get()
          .addShutdownHook(new CompositeServiceShutdownHook(serviceMaster), 30);
      YarnConfiguration conf = new YarnConfiguration();
      Options opts = new Options();
      opts.addOption(YARNFILE_OPTION, true, "HDFS path to JSON service " +
          "specification");
      opts.getOption(YARNFILE_OPTION).setRequired(true);
      GenericOptionsParser parser = new GenericOptionsParser(conf, opts, args);
      CommandLine cmdLine = parser.getCommandLine();
      serviceMaster.serviceDefPath = cmdLine.getOptionValue(YARNFILE_OPTION);
      serviceMaster.init(conf);
      serviceMaster.start();
    } catch (Throwable t) {
      LOG.error("Error starting service master", t);
      ExitUtil.terminate(1, "Error starting service master");
    }
  }
}

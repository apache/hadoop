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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator.PolicyGenerator;
import org.apache.hadoop.yarn.server.globalpolicygenerator.subclustercleaner.SubClusterCleaner;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.GPGWebApp;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.WebServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global Policy Generator (GPG) is a Yarn Federation component. By tuning the
 * Federation policies in Federation State Store, GPG overlooks the entire
 * federated cluster and ensures that the system is tuned and balanced all the
 * time.
 *
 * The GPG operates continuously but out-of-band from all cluster operations,
 * that allows to enforce global invariants, affect load balancing, trigger
 * draining of sub-clusters that will undergo maintenance, etc.
 */
public class GlobalPolicyGenerator extends CompositeService {

  public static final Logger LOG =
      LoggerFactory.getLogger(GlobalPolicyGenerator.class);

  // YARN Variables
  private static CompositeServiceShutdownHook gpgShutdownHook;
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private static final String METRICS_NAME = "Global Policy Generator";
  private static long gpgStartupTime = System.currentTimeMillis();

  // Federation Variables
  private GPGContext gpgContext;

  // Scheduler service that runs tasks periodically
  private ScheduledThreadPoolExecutor scheduledExecutorService;
  private SubClusterCleaner subClusterCleaner;
  private PolicyGenerator policyGenerator;
  private String webAppAddress;
  private JvmPauseMonitor pauseMonitor;
  private WebApp webApp;

  public GlobalPolicyGenerator() {
    super(GlobalPolicyGenerator.class.getName());
    this.gpgContext = new GPGContextImpl();
  }

  protected void doSecureLogin() throws IOException {
    Configuration config = getConfig();
    SecurityUtil.login(config, YarnConfiguration.GPG_KEYTAB,
        YarnConfiguration.GPG_PRINCIPAL, getHostName(config));
  }

  protected void initAndStart(Configuration conf, boolean hasToReboot) {
    // Remove the old hook if we are rebooting.
    if (hasToReboot && null != gpgShutdownHook) {
      ShutdownHookManager.get().removeShutdownHook(gpgShutdownHook);
    }
    gpgShutdownHook = new CompositeServiceShutdownHook(this);
    ShutdownHookManager.get().addShutdownHook(gpgShutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
    this.init(conf);
    this.start();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // Set up the context
    this.gpgContext.setStateStoreFacade(FederationStateStoreFacade.getInstance());
    GPGPolicyFacade gpgPolicyFacade =
        new GPGPolicyFacade(this.gpgContext.getStateStoreFacade(), conf);
    this.gpgContext.setPolicyFacade(gpgPolicyFacade);

    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
        conf.getInt(YarnConfiguration.GPG_SCHEDULED_EXECUTOR_THREADS,
            YarnConfiguration.DEFAULT_GPG_SCHEDULED_EXECUTOR_THREADS));
    this.subClusterCleaner = new SubClusterCleaner(conf, this.gpgContext);
    this.policyGenerator = new PolicyGenerator(conf, this.gpgContext);

    this.webAppAddress = WebAppUtils.getGPGWebAppURLWithoutScheme(conf);
    DefaultMetricsSystem.initialize(METRICS_NAME);
    JvmMetrics jm = JvmMetrics.initSingleton("GPG", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    // super.serviceInit after all services are added
    super.serviceInit(conf);
    WebServiceClient.initialize(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed GPG login", e);
    }

    super.serviceStart();

    // Scheduler SubClusterCleaner service
    Configuration config = getConfig();
    long scCleanerIntervalMs = config.getTimeDuration(
        YarnConfiguration.GPG_SUBCLUSTER_CLEANER_INTERVAL_MS,
        YarnConfiguration.DEFAULT_GPG_SUBCLUSTER_CLEANER_INTERVAL_MS, TimeUnit.MILLISECONDS);
    if (scCleanerIntervalMs > 0) {
      this.scheduledExecutorService.scheduleAtFixedRate(this.subClusterCleaner,
          0, scCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled sub-cluster cleaner with interval: {}",
          DurationFormatUtils.formatDurationISO(scCleanerIntervalMs));
    }

    // Schedule PolicyGenerator
    // We recommend using yarn.federation.gpg.policy.generator.interval
    // instead of yarn.federation.gpg.policy.generator.interval-ms

    // To ensure compatibility,
    // let's first obtain the value of "yarn.federation.gpg.policy.generator.interval-ms."
    long policyGeneratorIntervalMillis = 0L;
    String generatorIntervalMS = config.get(YarnConfiguration.GPG_POLICY_GENERATOR_INTERVAL_MS);
    if (generatorIntervalMS != null) {
      LOG.warn("yarn.federation.gpg.policy.generator.interval-ms is deprecated property, " +
          " we better set it yarn.federation.gpg.policy.generator.interval.");
      policyGeneratorIntervalMillis = Long.parseLong(generatorIntervalMS);
    }

    // If it is not available, let's retrieve
    // the value of "yarn.federation.gpg.policy.generator.interval" instead.
    if (policyGeneratorIntervalMillis == 0) {
      policyGeneratorIntervalMillis = config.getTimeDuration(
          YarnConfiguration.GPG_POLICY_GENERATOR_INTERVAL,
          YarnConfiguration.DEFAULT_GPG_POLICY_GENERATOR_INTERVAL, TimeUnit.MILLISECONDS);
    }

    if(policyGeneratorIntervalMillis > 0){
      this.scheduledExecutorService.scheduleAtFixedRate(this.policyGenerator,
          0, policyGeneratorIntervalMillis, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled policy-generator with interval: {}",
          DurationFormatUtils.formatDurationISO(policyGeneratorIntervalMillis));
    }
    startWepApp();
  }

  @Override
  protected void serviceStop() throws Exception {
    try {
      if (this.scheduledExecutorService != null
          && !this.scheduledExecutorService.isShutdown()) {
        this.scheduledExecutorService.shutdown();
        LOG.info("Stopped ScheduledExecutorService");
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown ScheduledExecutorService", e);
      throw e;
    }

    if (this.isStopping.getAndSet(true)) {
      return;
    }
    if (webApp != null) {
      webApp.stop();
    }
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
    WebServiceClient.destroy();
  }

  public String getName() {
    return "FederationGlobalPolicyGenerator";
  }

  public GPGContext getGPGContext() {
    return this.gpgContext;
  }

  @VisibleForTesting
  public void startWepApp() {
    Configuration configuration = getConfig();

    boolean enableCors = configuration.getBoolean(YarnConfiguration.GPG_WEBAPP_ENABLE_CORS_FILTER,
        YarnConfiguration.DEFAULT_GPG_WEBAPP_ENABLE_CORS_FILTER);

    if (enableCors) {
      configuration.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    // Always load pseudo authentication filter to parse "user.name" in an URL
    // to identify a HTTP request's user.
    boolean hasHadoopAuthFilterInitializer = false;
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    Class<?>[] initializersClasses = configuration.getClasses(filterInitializerConfKey);

    List<String> targets = new ArrayList<>();
    if (initializersClasses != null) {
      for (Class<?> initializer : initializersClasses) {
        if (initializer.getName().equals(AuthenticationFilterInitializer.class.getName())) {
          hasHadoopAuthFilterInitializer = true;
          break;
        }
        targets.add(initializer.getName());
      }
    }
    if (!hasHadoopAuthFilterInitializer) {
      targets.add(AuthenticationFilterInitializer.class.getName());
      configuration.set(filterInitializerConfKey, StringUtils.join(",", targets));
    }
    LOG.info("Instantiating GPGWebApp at {}.", webAppAddress);
    GPGWebApp gpgWebApp = new GPGWebApp(this);
    webApp = WebApps.$for("gpg").at(webAppAddress).start(gpgWebApp);
  }

  @SuppressWarnings("resource")
  public static void startGPG(String[] argv, Configuration conf) {
    boolean federationEnabled = conf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    if (federationEnabled) {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      StringUtils.startupShutdownMessage(GlobalPolicyGenerator.class, argv, LOG);
      GlobalPolicyGenerator globalPolicyGenerator = new GlobalPolicyGenerator();
      globalPolicyGenerator.initAndStart(conf, false);
    } else {
      LOG.warn("Federation is not enabled. The gpg cannot start.");
    }
  }

  /**
   * Returns the hostname for this Router. If the hostname is not
   * explicitly configured in the given config, then it is determined.
   *
   * @param config configuration
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the hostname cannot be determined
   */
  private String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(YarnConfiguration.GPG_KERBEROS_PRINCIPAL_HOSTNAME_KEY);
    if (name == null) {
      name = InetAddress.getLocalHost().getHostName();
    }
    return name;
  }

  public static void main(String[] argv) {
    try {
      startGPG(argv, new YarnConfiguration());
    } catch (Throwable t) {
      LOG.error("Error starting global policy generator", t);
      System.exit(-1);
    }
  }

  public static long getGPGStartupTime() {
    return gpgStartupTime;
  }

  @VisibleForTesting
  public WebApp getWebApp() {
    return webApp;
  }
}

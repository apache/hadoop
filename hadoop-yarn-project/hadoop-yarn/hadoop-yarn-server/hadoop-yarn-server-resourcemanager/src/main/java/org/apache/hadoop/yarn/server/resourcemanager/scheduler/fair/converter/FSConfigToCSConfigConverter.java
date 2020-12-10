/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSQueueConverter.QUEUE_MAX_AM_SHARE_DISABLED;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfigurationException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Converts Fair Scheduler configuration (site and fair-scheduler.xml)
 * to Capacity Scheduler. The mapping is not 100% perfect due to
 * feature gaps. These will be addressed in the future.
 */
public class FSConfigToCSConfigConverter {
  public static final Logger LOG = LoggerFactory.getLogger(
      FSConfigToCSConfigConverter.class.getName());
  private static final String YARN_SITE_XML = "yarn-site.xml";
  private static final String CAPACITY_SCHEDULER_XML =
      "capacity-scheduler.xml";
  private static final String FAIR_SCHEDULER_XML =
      "fair-scheduler.xml";

  public static final String WARNING_TEXT =
      "WARNING: This feature is experimental and not intended " +
          "for production use!";

  private Resource clusterResource;
  private boolean preemptionEnabled = false;
  private int queueMaxAppsDefault;
  private float queueMaxAMShareDefault;
  private Map<String, Integer> userMaxApps;
  private int userMaxAppsDefault;

  private boolean autoCreateChildQueues = false;
  private boolean sizeBasedWeight = false;
  private boolean userAsDefaultQueue = false;
  private ConversionOptions conversionOptions;
  private boolean drfUsed = false;

  private Configuration convertedYarnSiteConfig;
  private Configuration capacitySchedulerConfig;
  private FSConfigToCSConfigRuleHandler ruleHandler;
  private QueuePlacementConverter placementConverter;

  private OutputStream yarnSiteOutputStream;
  private OutputStream capacitySchedulerOutputStream;
  private boolean consoleMode = false;
  private boolean convertPlacementRules = false;



  public FSConfigToCSConfigConverter(FSConfigToCSConfigRuleHandler
      ruleHandler, ConversionOptions conversionOptions) {
    this.ruleHandler = ruleHandler;
    this.conversionOptions = conversionOptions;
    this.yarnSiteOutputStream = System.out;
    this.capacitySchedulerOutputStream = System.out;
    this.placementConverter = new QueuePlacementConverter();
  }

  public void convert(FSConfigToCSConfigConverterParams params)
      throws Exception {
    validateParams(params);
    prepareOutputFiles(params.getOutputDirectory(), params.isConsole());
    loadConversionRules(params.getConversionRulesConfig());
    Configuration inputYarnSiteConfig = getInputYarnSiteConfig(params);
    handleFairSchedulerConfig(params, inputYarnSiteConfig);

    this.clusterResource = getClusterResource(params);
    this.convertPlacementRules = params.isConvertPlacementRules();

    convert(inputYarnSiteConfig);
  }

  private void prepareOutputFiles(String outputDirectory, boolean console)
      throws FileNotFoundException {
    if (console) {
      LOG.info("Console mode is enabled, " + YARN_SITE_XML + " and" +
          " " + CAPACITY_SCHEDULER_XML + " will be only emitted " +
          "to the console!");
      this.consoleMode = true;
      return;
    }
    File yarnSiteXmlOutput = new File(outputDirectory,
        YARN_SITE_XML);
    File schedulerXmlOutput = new File(outputDirectory,
        CAPACITY_SCHEDULER_XML);
    LOG.info("Output directory for " + YARN_SITE_XML + " and" +
        " " + CAPACITY_SCHEDULER_XML + " is: {}", outputDirectory);

    this.yarnSiteOutputStream = new FileOutputStream(yarnSiteXmlOutput);
    this.capacitySchedulerOutputStream =
        new FileOutputStream(schedulerXmlOutput);
  }

  private void validateParams(FSConfigToCSConfigConverterParams params) {
    if (params.getYarnSiteXmlConfig() == null) {
      throw new PreconditionException("" + YARN_SITE_XML + " configuration " +
          "is not defined but it is mandatory!");
    } else if (params.getOutputDirectory() == null && !params.isConsole()) {
      throw new PreconditionException("Output directory configuration " +
          "is not defined but it is mandatory!");
    }
  }

  private Resource getClusterResource(
      FSConfigToCSConfigConverterParams params) {
    Resource resource = null;
    if (params.getClusterResource() != null) {
      ConfigurableResource configurableResource;
      try {
        configurableResource = FairSchedulerConfiguration
            .parseResourceConfigValue(params.getClusterResource());
      } catch (AllocationConfigurationException e) {
        throw new ConversionException("Error while parsing resource.", e);
      }
      resource = configurableResource.getResource();
    }
    return resource;
  }

  private void loadConversionRules(String rulesFile) throws IOException {
    if (rulesFile != null) {
      LOG.info("Reading conversion rules file from: " + rulesFile);
      ruleHandler.loadRulesFromFile(rulesFile);
    } else {
      LOG.info("Conversion rules file is not defined, " +
          "using default conversion config!");
    }

    ruleHandler.initPropertyActions();
  }

  private Configuration getInputYarnSiteConfig(
      FSConfigToCSConfigConverterParams params) {
    Configuration conf = new YarnConfiguration();
    conf.addResource(new Path(params.getYarnSiteXmlConfig()));
    return conf;
  }

  private void handleFairSchedulerConfig(
      FSConfigToCSConfigConverterParams params, Configuration conf) {
    String fairSchedulerXmlConfig = params.getFairSchedulerXmlConfig();

    // Don't override allocation file in conf yet, as it would ruin the second
    // condition here
    if (fairSchedulerXmlConfig != null) {
      LOG.info("Using explicitly defined " + FAIR_SCHEDULER_XML);
    } else if (conf.get(FairSchedulerConfiguration.ALLOCATION_FILE) != null) {
      LOG.info("Using " + FAIR_SCHEDULER_XML + " defined in " +
          YARN_SITE_XML + " by key: " +
          FairSchedulerConfiguration.ALLOCATION_FILE);
    } else {
      throw new PreconditionException("" + FAIR_SCHEDULER_XML +
          " is not defined neither in " + YARN_SITE_XML +
          "(with property: " + FairSchedulerConfiguration.ALLOCATION_FILE +
          ") nor directly with its own parameter!");
    }

    // We can now safely override allocation file in conf
    if (fairSchedulerXmlConfig != null) {
      conf.set(FairSchedulerConfiguration.ALLOCATION_FILE,
          params.getFairSchedulerXmlConfig());
    }
  }

  @VisibleForTesting
  void convert(Configuration inputYarnSiteConfig) throws Exception {
    System.out.println(WARNING_TEXT);

    // initialize Fair Scheduler
    RMContext ctx = new RMContextImpl();
    PlacementManager placementManager = new PlacementManager();
    ctx.setQueuePlacementManager(placementManager);

    // Prepare a separate config for the FS instance
    // to force the use of ConfiguredYarnAuthorizer, otherwise
    // it might use that of Ranger
    Configuration fsConfig = new Configuration(inputYarnSiteConfig);
    fsConfig.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
    fsConfig.setBoolean(FairSchedulerConfiguration.NO_TERMINAL_RULE_CHECK,
        conversionOptions.isNoRuleTerminalCheck());
    fsConfig.setClass(YarnConfiguration.YARN_AUTHORIZATION_PROVIDER,
        ConfiguredYarnAuthorizer.class, YarnAuthorizationProvider.class);
    FairScheduler fs = new FairScheduler();
    fs.setRMContext(ctx);
    fs.init(fsConfig);
    boolean havePlacementPolicies =
        checkPlacementPoliciesPresent(fs, inputYarnSiteConfig);

    drfUsed = isDrfUsed(fs);

    AllocationConfiguration allocConf = fs.getAllocationConfiguration();
    queueMaxAppsDefault = allocConf.getQueueMaxAppsDefault();
    userMaxAppsDefault = allocConf.getUserMaxAppsDefault();
    userMaxApps = allocConf.getUserMaxApps();
    queueMaxAMShareDefault = allocConf.getQueueMaxAMShareDefault();

    convertedYarnSiteConfig = new Configuration(false);
    capacitySchedulerConfig = new Configuration(false);

    convertYarnSiteXml(inputYarnSiteConfig, havePlacementPolicies);
    convertCapacitySchedulerXml(fs);

    if (consoleMode) {
      System.out.println("======= " + CAPACITY_SCHEDULER_XML + " =======");
    }
    capacitySchedulerConfig.writeXml(capacitySchedulerOutputStream);

    if (consoleMode) {
      System.out.println();
      System.out.println("======= " + YARN_SITE_XML + " =======");
    }
    convertedYarnSiteConfig.writeXml(yarnSiteOutputStream);
  }

  private void convertYarnSiteXml(Configuration inputYarnSiteConfig,
      boolean havePlacementPolicies) {
    FSYarnSiteConverter siteConverter =
        new FSYarnSiteConverter();
    siteConverter.convertSiteProperties(inputYarnSiteConfig,
        convertedYarnSiteConfig, drfUsed,
        conversionOptions.isEnableAsyncScheduler());

    // See docs: "allow-undeclared-pools" and "user-as-default-queue" are
    // ignored if we have placement rules
    autoCreateChildQueues =
        !havePlacementPolicies && siteConverter.isAutoCreateChildQueues();
    userAsDefaultQueue =
        !havePlacementPolicies && siteConverter.isUserAsDefaultQueue();
    preemptionEnabled = siteConverter.isPreemptionEnabled();
    sizeBasedWeight = siteConverter.isSizeBasedWeight();

    checkReservationSystem(inputYarnSiteConfig);
  }

  private void convertCapacitySchedulerXml(FairScheduler fs) {
    FSParentQueue rootQueue = fs.getQueueManager().getRootQueue();
    emitDefaultQueueMaxParallelApplications();
    emitDefaultUserMaxParallelApplications();
    emitUserMaxParallelApplications();
    emitDefaultMaxAMShare();

    FSQueueConverter queueConverter = FSQueueConverterBuilder.create()
        .withRuleHandler(ruleHandler)
        .withCapacitySchedulerConfig(capacitySchedulerConfig)
        .withPreemptionEnabled(preemptionEnabled)
        .withSizeBasedWeight(sizeBasedWeight)
        .withAutoCreateChildQueues(autoCreateChildQueues)
        .withClusterResource(clusterResource)
        .withQueueMaxAMShareDefault(queueMaxAMShareDefault)
        .withQueueMaxAppsDefault(queueMaxAppsDefault)
        .withConversionOptions(conversionOptions)
        .withDrfUsed(drfUsed)
        .build();

    queueConverter.convertQueueHierarchy(rootQueue);
    emitACLs(fs);

    if (convertPlacementRules) {
      LOG.info("Converting placement rules");
      PlacementManager placementManager =
          fs.getRMContext().getQueuePlacementManager();

      if (placementManager.getPlacementRules().size() > 0) {
        Map<String, String> properties =
            placementConverter.convertPlacementPolicy(placementManager,
                ruleHandler, userAsDefaultQueue);
        properties.forEach((k, v) -> capacitySchedulerConfig.set(k, v));
      }
    } else {
      LOG.info("Ignoring the conversion of placement rules");
    }
  }

  private void emitDefaultQueueMaxParallelApplications() {
    if (queueMaxAppsDefault != Integer.MAX_VALUE) {
      capacitySchedulerConfig.set(
          PREFIX + "max-parallel-apps",
          String.valueOf(queueMaxAppsDefault));
    }
  }

  private void emitDefaultUserMaxParallelApplications() {
    if (userMaxAppsDefault != Integer.MAX_VALUE) {
      capacitySchedulerConfig.set(
          PREFIX + "user.max-parallel-apps",
          String.valueOf(userMaxAppsDefault));
    }
  }

  private void emitUserMaxParallelApplications() {
    userMaxApps
        .forEach((user, apps) -> {
          capacitySchedulerConfig.setInt(
              PREFIX + "user." + user + ".max-parallel-apps", apps);
        });
  }

  private void emitDefaultMaxAMShare() {
    if (queueMaxAMShareDefault == QUEUE_MAX_AM_SHARE_DISABLED) {
      capacitySchedulerConfig.setFloat(
          CapacitySchedulerConfiguration.
            MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
            1.0f);
    } else {
      capacitySchedulerConfig.setFloat(
          CapacitySchedulerConfiguration.
            MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
          queueMaxAMShareDefault);
    }
  }

  private void emitACLs(FairScheduler fs) {
    fs.getAllocationConfiguration().getQueueAcls()
        .forEach(this::generateQueueAcl);
  }

  private void generateQueueAcl(String queue,
      Map<AccessType, AccessControlList> access) {
    AccessControlList submitAcls = access.get(AccessType.SUBMIT_APP);
    AccessControlList adminAcls = access.get(AccessType.ADMINISTER_QUEUE);

    if (!submitAcls.getGroups().isEmpty() ||
        !submitAcls.getUsers().isEmpty()) {
      capacitySchedulerConfig.set(PREFIX + queue + ".acl_submit_applications",
          submitAcls.getAclString());
    }

    if (!adminAcls.getGroups().isEmpty() ||
        !adminAcls.getUsers().isEmpty()) {
      capacitySchedulerConfig.set(PREFIX + queue + ".acl_administer_queue",
          adminAcls.getAclString());
    }
  }

  private void checkReservationSystem(Configuration conf) {
    if (conf.getBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_ENABLE)) {
      ruleHandler.handleReservationSystem();
    }
  }

  private boolean isDrfUsed(FairScheduler fs) {
    FSQueue rootQueue = fs.getQueueManager().getRootQueue();
    AllocationConfiguration allocConf = fs.getAllocationConfiguration();

    String defaultPolicy = allocConf.getDefaultSchedulingPolicy().getName();

    if (DominantResourceFairnessPolicy.NAME.equals(defaultPolicy)) {
      return true;
    } else {
      return isDrfUsedOnQueueLevel(rootQueue);
    }
  }

  private boolean isDrfUsedOnQueueLevel(FSQueue queue) {
    String policy = queue.getPolicy().getName();
    boolean usesDrf = DominantResourceFairnessPolicy.NAME.equals(policy);

    if (usesDrf) {
      return true;
    } else {
      List<FSQueue> children = queue.getChildQueues();

      if (children != null) {
        for (FSQueue child : children) {
          usesDrf |= isDrfUsedOnQueueLevel(child);
        }
      }

      return usesDrf;
    }
  }

  @VisibleForTesting
  Resource getClusterResource() {
    return clusterResource;
  }

  @VisibleForTesting
  public void setClusterResource(Resource clusterResource) {
    this.clusterResource = clusterResource;
  }

  @VisibleForTesting
  FSConfigToCSConfigRuleHandler getRuleHandler() {
    return ruleHandler;
  }

  @VisibleForTesting
  Configuration getYarnSiteConfig() {
    return convertedYarnSiteConfig;
  }

  @VisibleForTesting
  Configuration getCapacitySchedulerConfig() {
    return capacitySchedulerConfig;
  }

  @VisibleForTesting
  void setConvertPlacementRules(boolean convertPlacementRules) {
    this.convertPlacementRules = convertPlacementRules;
  }

  @VisibleForTesting
  void setPlacementConverter(QueuePlacementConverter converter) {
    this.placementConverter = converter;
  }
  /*
   * Determines whether <queuePlacementPolicy> is present
   * in the allocation file or not.
   *
   * Note that placementManager.getPlacementRules.size()
   * doesn't work - by default, "allow-undeclared-pools" and
   * "user-as-default-queue" are translated to policies internally
   * inside QueuePlacementPolicy.fromConfiguration().
   *
   */
  private boolean checkPlacementPoliciesPresent(FairScheduler scheduler,
      Configuration inputYarnSiteConfig)
      throws RuntimeException {

    try (AllocationFileLoaderService loader =
        new AllocationFileLoaderService(scheduler)){

      Path allocFilePath = loader.getAllocationFile(inputYarnSiteConfig);
      FileSystem fs = allocFilePath.getFileSystem(inputYarnSiteConfig);

      DocumentBuilderFactory docBuilderFactory =
          DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = builder.parse(fs.open(allocFilePath));
      Element root = doc.getDocumentElement();

      NodeList elements = root.getChildNodes();

      AllocationFileParser allocationFileParser =
          new AllocationFileParser(elements);
      allocationFileParser.parse();
      docBuilderFactory.setIgnoringComments(true);

      return
          allocationFileParser.getQueuePlacementPolicy().isPresent();
    } catch (Exception e) {
      throw new PreconditionException("Unable to parse allocation file", e);
    }
  }
}

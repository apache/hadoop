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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfigurationException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.ConfigurableResource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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
  private boolean autoCreateChildQueues = false;
  private boolean sizeBasedWeight = false;
  private boolean userAsDefaultQueue = false;

  private Configuration yarnSiteConfig;
  private Configuration capacitySchedulerConfig;
  private FSConfigToCSConfigRuleHandler ruleHandler;

  private OutputStream yarnSiteOutputStream;
  private OutputStream capacitySchedulerOutputStream;
  private boolean consoleMode = false;

  public FSConfigToCSConfigConverter(FSConfigToCSConfigRuleHandler
      ruleHandler) {
    this.ruleHandler = ruleHandler;
    this.yarnSiteOutputStream = System.out;
    this.capacitySchedulerOutputStream = System.out;
  }

  public void convert(FSConfigToCSConfigConverterParams params)
      throws Exception {
    validateParams(params);
    prepareOutputFiles(params.getOutputDirectory(), params.isConsole());
    loadConversionRules(params.getConversionRulesConfig());
    Configuration conf = createConfiguration(params);
    handleFairSchedulerConfig(params, conf);

    this.clusterResource = getClusterResource(params);
    convert(conf);
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
      this.ruleHandler.loadRulesFromFile(rulesFile);
    } else {
      LOG.info("Conversion rules file is not defined, " +
          "using default conversion config!");
    }
  }

  private Configuration createConfiguration(
      FSConfigToCSConfigConverterParams params) {
    Configuration conf = new YarnConfiguration();
    conf.addResource(new Path(params.getYarnSiteXmlConfig()));
    conf.setBoolean(FairSchedulerConfiguration.MIGRATION_MODE, true);
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
  void convert(Configuration conf) throws Exception {
    System.out.println(WARNING_TEXT);
    
    // initialize Fair Scheduler
    RMContext ctx = new RMContextImpl();
    PlacementManager placementManager = new PlacementManager();
    ctx.setQueuePlacementManager(placementManager);

    FairScheduler fs = new FairScheduler();
    fs.setRMContext(ctx);
    fs.init(conf);

    AllocationConfiguration allocConf = fs.getAllocationConfiguration();
    queueMaxAppsDefault = allocConf.getQueueMaxAppsDefault();
    queueMaxAMShareDefault = allocConf.getQueueMaxAMShareDefault();

    yarnSiteConfig = new Configuration(false);
    capacitySchedulerConfig = new Configuration(false);

    checkUserMaxApps(allocConf);
    checkUserMaxAppsDefault(allocConf);

    convertYarnSiteXml(conf);
    convertCapacitySchedulerXml(fs);

    if (consoleMode) {
      System.out.println("======= " + CAPACITY_SCHEDULER_XML + " =======");
    }
    capacitySchedulerConfig.writeXml(capacitySchedulerOutputStream);

    if (consoleMode) {
      System.out.println();
      System.out.println("======= " + YARN_SITE_XML + " =======");
    }
    yarnSiteConfig.writeXml(yarnSiteOutputStream);
  }

  @VisibleForTesting
  void setYarnSiteOutputStream(OutputStream out) {
    this.yarnSiteOutputStream = out;
  }

  @VisibleForTesting
  void setCapacitySchedulerConfigOutputStream(OutputStream out) {
    this.capacitySchedulerOutputStream = out;
  }

  private void convertYarnSiteXml(Configuration conf) {
    FSYarnSiteConverter siteConverter =
        new FSYarnSiteConverter();
    siteConverter.convertSiteProperties(conf, yarnSiteConfig);

    autoCreateChildQueues = siteConverter.isAutoCreateChildQueues();
    preemptionEnabled = siteConverter.isPreemptionEnabled();
    sizeBasedWeight = siteConverter.isSizeBasedWeight();
    userAsDefaultQueue = siteConverter.isUserAsDefaultQueue();

    checkReservationSystem(conf);
  }

  private void convertCapacitySchedulerXml(FairScheduler fs) {
    FSParentQueue rootQueue = fs.getQueueManager().getRootQueue();
    emitDefaultMaxApplications();
    emitDefaultMaxAMShare();
    FSQueueConverter queueConverter = new FSQueueConverter(ruleHandler,
        capacitySchedulerConfig,
        preemptionEnabled,
        sizeBasedWeight,
        autoCreateChildQueues,
        clusterResource,
        queueMaxAMShareDefault,
        queueMaxAppsDefault);
    queueConverter.convertQueueHierarchy(rootQueue);
    emitACLs(fs);

    PlacementManager placementManager =
        fs.getRMContext().getQueuePlacementManager();

    if (placementManager.getPlacementRules().size() > 0) {
      QueuePlacementConverter placementConverter =
          new QueuePlacementConverter();
      Map<String, String> properties =
          placementConverter.convertPlacementPolicy(placementManager,
              ruleHandler, userAsDefaultQueue);
      properties.forEach((k, v) -> capacitySchedulerConfig.set(k, v));
    }

    // Validate ordering policy
    if (queueConverter.isDrfPolicyUsedOnQueueLevel()) {
      if (queueConverter.isFifoOrFairSharePolicyUsed()) {
        throw new ConversionException(
            "DRF ordering policy cannot be used together with fifo/fair");
      } else {
        capacitySchedulerConfig.set(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
            DominantResourceCalculator.class.getCanonicalName());
      }
    }
  }

  private void emitDefaultMaxApplications() {
    if (queueMaxAppsDefault != Integer.MAX_VALUE) {
      capacitySchedulerConfig.set(
          CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
          String.valueOf(queueMaxAppsDefault));
    }
  }

  private void emitDefaultMaxAMShare() {
    capacitySchedulerConfig.set(
        CapacitySchedulerConfiguration.
          MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT,
        String.valueOf(queueMaxAMShareDefault));
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

  private void checkUserMaxApps(AllocationConfiguration allocConf) {
    if (allocConf.getUserMaxApps() != null
        && allocConf.getUserMaxApps().size() > 0) {
      ruleHandler.handleUserMaxApps();
    }
  }

  private void checkUserMaxAppsDefault(AllocationConfiguration allocConf) {
    if (allocConf.getUserMaxAppsDefault() > 0) {
      ruleHandler.handleUserMaxAppsDefault();
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
}

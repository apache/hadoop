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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration.parseResourceConfigValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.impl.LightWeightResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;

public class TestFairSchedulerConfiguration {

  private static final String A_CUSTOM_RESOURCE = "a-custom-resource";

  private static class CustomResourceTypesConfigurationProvider
      extends LocalConfigurationProvider {

    @Override
    public InputStream getConfigurationInputStream(Configuration bootstrapConf,
        String name) throws YarnException, IOException {
      if (YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE.equals(name)) {
        return new ByteArrayInputStream((
            "<configuration>\n" +
            " <property>\n" +
            "   <name>yarn.resource-types</name>\n" +
            "   <value>" + A_CUSTOM_RESOURCE + "</value>\n" +
            " </property>\n" +
            " <property>\n" +
            "   <name>yarn.resource-types.a-custom-resource.units</name>\n" +
            "   <value>k</value>\n" +
            " </property>\n" +
            "</configuration>\n")
                .getBytes());
      } else {
        return super.getConfigurationInputStream(bootstrapConf, name);
      }
    }
  }

  private static class TestAppender extends AppenderSkeleton {

    private final List<LoggingEvent> logEvents = new CopyOnWriteArrayList<>();

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    public void close() {
    }

    @Override
    protected void append(LoggingEvent arg0) {
      logEvents.add(arg0);
    }

    private List<LoggingEvent> getLogEvents() {
      return logEvents;
    }
  }

  @Test
  public void testParseResourceConfigValue() throws Exception {
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("2 vcores, 1024 mb").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024 mb, 2 vcores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("2vcores,1024mb").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024mb,2vcores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024   mb, 2    vcores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("1024 Mb, 2 vCores").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  1024 mb, 2 vcores  ").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  1024.3 mb, 2.35 vcores  ").getResource());
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  1024. mb, 2. vcores  ").getResource());

    Resource clusterResource = BuilderUtils.newResource(2048, 4);
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50% memory, 50% cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50% Memory, 50% CpU").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50%").getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 4),
        parseResourceConfigValue("50% memory, 100% cpu").
        getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 4),
        parseResourceConfigValue(" 100% cpu, 50% memory").
        getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 0),
        parseResourceConfigValue("50% memory, 0% cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50 % memory, 50 % cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50%memory,50%cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("  50  %  memory,  50  %  cpu  ").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(1024, 2),
        parseResourceConfigValue("50.% memory, 50.% cpu").
            getResource(clusterResource));

    clusterResource =  BuilderUtils.newResource(1024 * 10, 4);
    assertEquals(BuilderUtils.newResource((int)(1024 * 10 * 0.109), 2),
        parseResourceConfigValue("10.9% memory, 50.6% cpu").
            getResource(clusterResource));
  }
  
  @Test(expected = AllocationConfigurationException.class)
  public void testNoUnits() throws Exception {
    parseResourceConfigValue("1024");
  }
  
  @Test(expected = AllocationConfigurationException.class)
  public void testOnlyMemory() throws Exception {
    parseResourceConfigValue("1024mb");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testOnlyCPU() throws Exception {
    parseResourceConfigValue("1024vcores");
  }
  
  @Test(expected = AllocationConfigurationException.class)
  public void testGibberish() throws Exception {
    parseResourceConfigValue("1o24vc0res");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testNoUnitsPercentage() throws Exception {
    parseResourceConfigValue("95%, 50% memory");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testInvalidNumPercentage() throws Exception {
    parseResourceConfigValue("95A% cpu, 50% memory");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testCpuPercentageMemoryAbsolute() throws Exception {
    parseResourceConfigValue("50% cpu, 1024 mb");
  }

  @Test(expected = AllocationConfigurationException.class)
  public void testMemoryPercentageCpuAbsolute() throws Exception {
    parseResourceConfigValue("50% memory, 2 vcores");
  }

  @Test
  public void testAllocationIncrementMemoryDefaultUnit() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
        ResourceInformation.MEMORY_MB.getName() +
        FairSchedulerConfiguration.INCREMENT_ALLOCATION, "256");
    FairSchedulerConfiguration fsc = new FairSchedulerConfiguration(conf);
    Resource minimum = Resources.createResource(0L, 0);
    Resource maximum =
        Resources.createResource(Long.MAX_VALUE, Integer.MAX_VALUE);
    Resource increment = fsc.getIncrementAllocation();
    DominantResourceCalculator resourceCalculator =
        new DominantResourceCalculator();
    assertEquals(1024L, resourceCalculator.normalize(
        Resources.createResource(769L), minimum, maximum, increment)
          .getMemorySize());
    assertEquals(1024L, resourceCalculator.normalize(
        Resources.createResource(1023L), minimum, maximum, increment)
          .getMemorySize());
    assertEquals(1024L, resourceCalculator.normalize(
        Resources.createResource(1024L), minimum, maximum, increment)
          .getMemorySize());
    assertEquals(1280L, resourceCalculator.normalize(
        Resources.createResource(1025L), minimum, maximum, increment)
          .getMemorySize());
  }

  @Test
  public void testAllocationIncrementMemoryNonDefaultUnit() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
        ResourceInformation.MEMORY_MB.getName() +
        FairSchedulerConfiguration.INCREMENT_ALLOCATION, "1 Gi");
    FairSchedulerConfiguration fsc = new FairSchedulerConfiguration(conf);
    Resource minimum = Resources.createResource(0L, 0);
    Resource maximum =
        Resources.createResource(Long.MAX_VALUE, Integer.MAX_VALUE);
    Resource increment = fsc.getIncrementAllocation();
    DominantResourceCalculator resourceCalculator =
        new DominantResourceCalculator();
    assertEquals(1024L, resourceCalculator.normalize(
        Resources.createResource(1023L), minimum, maximum, increment)
          .getMemorySize());
    assertEquals(1024L, resourceCalculator.normalize(
        Resources.createResource(1024L), minimum, maximum, increment)
          .getMemorySize());
    assertEquals(2048L, resourceCalculator.normalize(
        Resources.createResource(1025L), minimum, maximum, increment)
          .getMemorySize());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testAllocationIncrementInvalidUnit() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
        ResourceInformation.MEMORY_MB.getName() +
        FairSchedulerConfiguration.INCREMENT_ALLOCATION, "1 Xi");
    new FairSchedulerConfiguration(conf).getIncrementAllocation();
  }

  @Test
  public void testAllocationIncrementVCoreNoUnit() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
        ResourceInformation.VCORES.getName() +
        FairSchedulerConfiguration.INCREMENT_ALLOCATION, "10");
    FairSchedulerConfiguration fsc = new FairSchedulerConfiguration(conf);
    Resource min = Resources.createResource(0L, 0);
    Resource max = Resources.createResource(Long.MAX_VALUE, Integer.MAX_VALUE);
    Resource increment = fsc.getIncrementAllocation();
    DominantResourceCalculator resourceCalculator =
        new DominantResourceCalculator();
    assertEquals(10, resourceCalculator.normalize(
        Resources.createResource(0L, 9), min, max, increment)
          .getVirtualCores());
    assertEquals(10, resourceCalculator.normalize(
        Resources.createResource(0L, 10), min, max, increment)
          .getVirtualCores());
    assertEquals(20, resourceCalculator.normalize(
        Resources.createResource(0L, 11), min, max, increment)
          .getVirtualCores());
  }

  @Test
  public void testAllocationIncrementVCoreWithUnit() throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
        ResourceInformation.VCORES.getName() +
        FairSchedulerConfiguration.INCREMENT_ALLOCATION, "1k");
    FairSchedulerConfiguration fsc = new FairSchedulerConfiguration(conf);
    Resource min = Resources.createResource(0L, 0);
    Resource max = Resources.createResource(Long.MAX_VALUE, Integer.MAX_VALUE);
    Resource increment = fsc.getIncrementAllocation();
    DominantResourceCalculator resourceCalculator =
        new DominantResourceCalculator();
    assertEquals(1000, resourceCalculator.normalize(
        Resources.createResource(0L, 999), min, max, increment)
          .getVirtualCores());
    assertEquals(1000, resourceCalculator.normalize(
        Resources.createResource(0L, 1000), min, max, increment)
          .getVirtualCores());
    assertEquals(2000, resourceCalculator.normalize(
        Resources.createResource(0L, 1001), min, max, increment)
          .getVirtualCores());
  }

  @Test
  public void testAllocationIncrementCustomResource() throws Exception {
    try {
      initResourceTypes();
      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.RESOURCE_TYPES + ".a-custom-resource" +
          FairSchedulerConfiguration.INCREMENT_ALLOCATION, "10");
      FairSchedulerConfiguration fsc = new FairSchedulerConfiguration(conf);
      Resource increment = fsc.getIncrementAllocation();
      DominantResourceCalculator calculator =
          new DominantResourceCalculator();
      Resource min = Resources.createResource(0L, 0);
      Resource max = Resource.newInstance(Long.MAX_VALUE,
          Integer.MAX_VALUE, Collections.singletonMap(A_CUSTOM_RESOURCE,
              Long.MAX_VALUE / UnitsConversionUtil.convert("k", "", 1L)));
      assertEquals(customResourceInformation(10000L, ""),
          calculator.normalize(customResource(9999L, ""), min, max, increment)
            .getResourceInformation(A_CUSTOM_RESOURCE));
      assertEquals(customResourceInformation(10000L, ""),
          calculator.normalize(customResource(10000L, ""), min, max, increment)
            .getResourceInformation(A_CUSTOM_RESOURCE));
      assertEquals(customResourceInformation(20000L, ""),
          calculator.normalize(customResource(10001L, ""), min, max, increment)
            .getResourceInformation(A_CUSTOM_RESOURCE));
      assertEquals(customResourceInformation(10L, "k"),
          calculator.normalize(customResource(9L, "k"), min, max, increment)
            .getResourceInformation(A_CUSTOM_RESOURCE));
      assertEquals(customResourceInformation(10L, "k"),
          calculator.normalize(customResource(10L, "k"), min, max, increment)
            .getResourceInformation(A_CUSTOM_RESOURCE));
      assertEquals(customResourceInformation(20L, "k"),
          calculator.normalize(customResource(11L, "k"), min, max, increment)
            .getResourceInformation(A_CUSTOM_RESOURCE));
    } finally {
      ResourceUtils.resetResourceTypes(new Configuration());
    }
  }

  private Resource customResource(long value, String units) {
    return new LightWeightResource(0L, 0, new ResourceInformation[] {
        null, null, customResourceInformation(value, units) });
  }

  private ResourceInformation customResourceInformation(long value,
      String units) {
    return ResourceInformation.newInstance(A_CUSTOM_RESOURCE, units, value,
        ResourceTypes.COUNTABLE, 0L, Long.MAX_VALUE);
  }

  private void initResourceTypes() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        CustomResourceTypesConfigurationProvider.class.getName());
    ResourceUtils.resetResourceTypes(conf);
  }

  @Test
  public void testMemoryIncrementConfiguredViaMultipleProperties() {
    TestAppender testAppender = new TestAppender();
    Log4JLogger logger = (Log4JLogger) FairSchedulerConfiguration.LOG;
    logger.getLogger().addAppender(testAppender);
    try {
      Configuration conf = new Configuration();
      conf.set("yarn.scheduler.increment-allocation-mb", "7");
      conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
          ResourceInformation.MEMORY_MB.getName() +
          FairSchedulerConfiguration.INCREMENT_ALLOCATION, "13");
      FairSchedulerConfiguration fsc = new FairSchedulerConfiguration(conf);
      Resource increment = fsc.getIncrementAllocation();
      Assert.assertEquals(13L, increment.getMemorySize());
      assertTrue("Warning message is not logged when specifying memory " +
          "increment via multiple properties",
          testAppender.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && ("Configuration " +
              "yarn.resource-types.memory-mb.increment-allocation=13 is " +
              "overriding the yarn.scheduler.increment-allocation-mb=7 " +
              "property").equals(e.getMessage())));
    } finally {
      logger.getLogger().removeAppender(testAppender);
    }
  }

  @Test
  public void testCpuIncrementConfiguredViaMultipleProperties() {
    TestAppender testAppender = new TestAppender();
    Log4JLogger logger = (Log4JLogger) FairSchedulerConfiguration.LOG;
    logger.getLogger().addAppender(testAppender);
    try {
      Configuration conf = new Configuration();
      conf.set("yarn.scheduler.increment-allocation-vcores", "7");
      conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
          ResourceInformation.VCORES.getName() +
          FairSchedulerConfiguration.INCREMENT_ALLOCATION, "13");
      FairSchedulerConfiguration fsc = new FairSchedulerConfiguration(conf);
      Resource increment = fsc.getIncrementAllocation();
      Assert.assertEquals(13, increment.getVirtualCores());
      assertTrue("Warning message is not logged when specifying CPU vCores " +
          "increment via multiple properties",
          testAppender.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && ("Configuration " +
              "yarn.resource-types.vcores.increment-allocation=13 is " +
              "overriding the yarn.scheduler.increment-allocation-vcores=7 " +
              "property").equals(e.getMessage())));
    } finally {
      logger.getLogger().removeAppender(testAppender);
    }
  }
}

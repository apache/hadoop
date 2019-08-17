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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.impl.LightWeightResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.CustomResourceTypesConfigurationProvider;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration.parseResourceConfigValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests fair scheduler configuration.
 */
public class TestFairSchedulerConfiguration {

  private static final String A_CUSTOM_RESOURCE = "a-custom-resource";

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

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private void expectMissingResource(String resource) {
    exception.expect(AllocationConfigurationException.class);
    exception.expectMessage("Missing resource: " + resource);
  }

  private void expectNegativePercentageOldStyle() {
    exception.expect(AllocationConfigurationException.class);
    exception.expectMessage("percentage should not be negative");
  }

  private void expectNegativePercentageNewStyle() {
    exception.expect(AllocationConfigurationException.class);
    exception.expectMessage("is either not a non-negative number");
  }

  private void expectNegativeValueOfResource(String resource) {
    exception.expect(AllocationConfigurationException.class);
    exception.expectMessage("Invalid value of " + resource);
  }

  @Test
  public void testParseResourceConfigValue() throws Exception {
    Resource expected = BuilderUtils.newResource(5 * 1024, 2);
    Resource clusterResource = BuilderUtils.newResource(10 * 1024, 4);

    assertEquals(expected,
        parseResourceConfigValue("2 vcores, 5120 mb").getResource());
    assertEquals(expected,
        parseResourceConfigValue("5120 mb, 2 vcores").getResource());
    assertEquals(expected,
        parseResourceConfigValue("2vcores,5120mb").getResource());
    assertEquals(expected,
        parseResourceConfigValue("5120mb,2vcores").getResource());
    assertEquals(expected,
        parseResourceConfigValue("5120mb   mb, 2    vcores").getResource());
    assertEquals(expected,
        parseResourceConfigValue("5120 Mb, 2 vCores").getResource());
    assertEquals(expected,
        parseResourceConfigValue("  5120 mb, 2 vcores  ").getResource());
    assertEquals(expected,
        parseResourceConfigValue("  5120.3 mb, 2.35 vcores  ").getResource());
    assertEquals(expected,
        parseResourceConfigValue("  5120. mb, 2. vcores  ").getResource());

    assertEquals(expected,
        parseResourceConfigValue("50% memory, 50% cpu").
            getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("50% Memory, 50% CpU").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(5 * 1024, 4),
        parseResourceConfigValue("50% memory, 100% cpu").
        getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(5 * 1024, 4),
        parseResourceConfigValue(" 100% cpu, 50% memory").
        getResource(clusterResource));
    assertEquals(BuilderUtils.newResource(5 * 1024, 0),
        parseResourceConfigValue("50% memory, 0% cpu").
            getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("50 % memory, 50 % cpu").
            getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("50%memory,50%cpu").
            getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("  50  %  memory,  50  %  cpu  ").
            getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("50.% memory, 50.% cpu").
            getResource(clusterResource));
    assertEquals(BuilderUtils.newResource((int)(1024 * 10 * 0.109), 2),
        parseResourceConfigValue("10.9% memory, 50.6% cpu").
            getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("50%").getResource(clusterResource));

    Configuration conf = new Configuration();

    conf.set(YarnConfiguration.RESOURCE_TYPES, "test1");
    ResourceUtils.resetResourceTypes(conf);

    clusterResource = BuilderUtils.newResource(10 * 1024, 4);
    expected = BuilderUtils.newResource(5 * 1024, 2);
    expected.setResourceValue("test1", Long.MAX_VALUE);

    assertEquals(expected,
        parseResourceConfigValue("vcores=2, memory-mb=5120").getResource());
    assertEquals(expected,
        parseResourceConfigValue("memory-mb=5120, vcores=2").getResource());
    assertEquals(expected,
        parseResourceConfigValue("vcores=2,memory-mb=5120").getResource());
    assertEquals(expected, parseResourceConfigValue(" vcores = 2 , "
            + "memory-mb = 5120 ").getResource());

    expected.setResourceValue("test1", 0L);

    assertEquals(expected,
        parseResourceConfigValue("vcores=2, memory-mb=5120", 0L).getResource());
    assertEquals(expected,
        parseResourceConfigValue("memory-mb=5120, vcores=2", 0L).getResource());
    assertEquals(expected,
        parseResourceConfigValue("vcores=2,memory-mb=5120", 0L).getResource());
    assertEquals(expected,
        parseResourceConfigValue(" vcores = 2 , memory-mb = 5120 ",
            0L).getResource());

    clusterResource.setResourceValue("test1", 8L);
    expected.setResourceValue("test1", 4L);

    assertEquals(expected,
        parseResourceConfigValue("50%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("vcores=2, memory-mb=5120, "
            + "test1=4").getResource());
    assertEquals(expected,
        parseResourceConfigValue("test1=4, vcores=2, "
            + "memory-mb=5120").getResource());
    assertEquals(expected,
        parseResourceConfigValue("memory-mb=5120, test1=4, "
            + "vcores=2").getResource());
    assertEquals(expected,
        parseResourceConfigValue("vcores=2,memory-mb=5120,"
            + "test1=4").getResource());
    assertEquals(expected,
        parseResourceConfigValue(" vcores = 2 , memory-mb = 5120 , "
            + "test1 = 4 ").getResource());

    expected = BuilderUtils.newResource(4 * 1024, 3);
    expected.setResourceValue("test1", 8L);

    assertEquals(expected,
        parseResourceConfigValue("vcores=75%, "
            + "memory-mb=40%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("memory-mb=40%, "
            + "vcores=75%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("vcores=75%,"
            + "memory-mb=40%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue(" vcores = 75 % , "
            + "memory-mb = 40 % ").getResource(clusterResource));

    expected.setResourceValue("test1", 4L);

    assertEquals(expected,
        parseResourceConfigValue("vcores=75%, memory-mb=40%, "
            + "test1=50%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("test1=50%, vcores=75%, "
            + "memory-mb=40%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("memory-mb=40%, test1=50%, "
            + "vcores=75%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue("vcores=75%,memory-mb=40%,"
            + "test1=50%").getResource(clusterResource));
    assertEquals(expected,
        parseResourceConfigValue(" vcores = 75 % , memory-mb = 40 % , "
            + "test1 = 50 % ").getResource(clusterResource));
  }

  @Test
  public void testNoUnits() throws Exception {
    expectMissingResource("mb");
    parseResourceConfigValue("1024");
  }

  @Test
  public void testOnlyMemory() throws Exception {
    expectMissingResource("vcores");
    parseResourceConfigValue("1024mb");
  }

  @Test
  public void testOnlyCPU() throws Exception {
    expectMissingResource("mb");
    parseResourceConfigValue("1024vcores");
  }

  @Test
  public void testGibberish() throws Exception {
    expectMissingResource("mb");
    parseResourceConfigValue("1o24vc0res");
  }

  @Test
  public void testNoUnitsPercentage() throws Exception {
    expectMissingResource("cpu");
    parseResourceConfigValue("95%, 50% memory");
  }

  @Test
  public void testInvalidNumPercentage() throws Exception {
    expectMissingResource("cpu");
    parseResourceConfigValue("95A% cpu, 50% memory");
  }

  @Test
  public void testCpuPercentageMemoryAbsolute() throws Exception {
    expectMissingResource("memory");
    parseResourceConfigValue("50% cpu, 1024 mb");
  }

  @Test
  public void testMemoryPercentageCpuAbsolute() throws Exception {
    expectMissingResource("cpu");
    parseResourceConfigValue("50% memory, 2 vcores");
  }

  @Test
  public void testMemoryPercentageNegativeValue() throws Exception {
    expectNegativePercentageOldStyle();
    parseResourceConfigValue("-10% memory, 50% cpu");
  }

  @Test
  public void testCpuPercentageNegativeValue() throws Exception {
    expectNegativePercentageOldStyle();
    parseResourceConfigValue("10% memory, -10% cpu");
  }

  @Test
  public void testMemoryAndCpuPercentageNegativeValue() throws Exception {
    expectNegativePercentageOldStyle();
    parseResourceConfigValue("-20% memory, -10% cpu");
  }

  @Test
  public void testCpuPercentageMemoryAbsoluteCpuNegative() throws Exception {
    expectMissingResource("memory");
    parseResourceConfigValue("-50% cpu, 1024 mb");
  }

  @Test
  public void testCpuPercentageMemoryAbsoluteMemoryNegative() throws Exception {
    expectMissingResource("memory");
    parseResourceConfigValue("50% cpu, -1024 mb");
  }

  @Test
  public void testMemoryPercentageCpuAbsoluteCpuNegative() throws Exception {
    expectMissingResource("cpu");
    parseResourceConfigValue("50% memory, -2 vcores");
  }

  @Test
  public void testMemoryPercentageCpuAbsoluteMemoryNegative() throws Exception {
    expectNegativePercentageOldStyle();
    parseResourceConfigValue("-50% memory, 2 vcores");
  }


  @Test
  public void testAbsoluteVcoresNegative() throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue("-2 vcores,5120 mb");
  }

  @Test
  public void testAbsoluteMemoryNegative() throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue("2 vcores,-5120 mb");
  }

  @Test
  public void testAbsoluteVcoresNegativeWithSpaces() throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue("-2 vcores, 5120 mb");
  }

  @Test
  public void testAbsoluteMemoryNegativeWithSpaces() throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue("2 vcores, -5120 mb");
  }

  @Test
  public void testAbsoluteVcoresNegativeWithMoreSpaces() throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue("5120mb   mb, -2    vcores");
  }

  @Test
  public void testAbsoluteMemoryNegativeWithMoreSpaces() throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue("-5120mb   mb, 2    vcores");
  }

  @Test
  public void testAbsoluteVcoresNegativeFractional() throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue("  5120.3 mb, -2.35 vcores  ");
  }

  @Test
  public void testAbsoluteMemoryNegativeFractional() throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue("  -5120.3 mb, 2.35 vcores  ");
  }

  @Test
  public void testParseNewStyleResourceMemoryNegative() throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue("memory-mb=-5120,vcores=2");
  }

  @Test
  public void testParseNewStyleResourceVcoresNegative() throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue("memory-mb=5120,vcores=-2");
  }

  @Test
  public void testParseNewStyleResourceMemoryNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue("memory-mb=-5120, vcores=2");
  }

  @Test
  public void testParseNewStyleResourceVcoresNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue("memory-mb=5120, vcores=-2");
  }

  @Test
  public void testParseNewStyleResourceMemoryNegativeWithMoreSpaces()
      throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue(" vcores = 2 ,  memory-mb = -5120 ");
  }

  @Test
  public void testParseNewStyleResourceVcoresNegativeWithMoreSpaces()
      throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue(" vcores = -2 ,  memory-mb = 5120 ");
  }

  @Test
  public void testParseNewStyleResourceWithCustomResourceMemoryNegative()
      throws Exception {
    expectNegativeValueOfResource("memory");
    parseResourceConfigValue("vcores=2,memory-mb=-5120,test1=4");
  }

  @Test
  public void testParseNewStyleResourceWithCustomResourceVcoresNegative()
      throws Exception {
    expectNegativeValueOfResource("vcores");
    parseResourceConfigValue("vcores=-2,memory-mb=-5120,test1=4");
  }

  @Test
  public void testParseNewStyleResourceWithCustomResourceNegative()
      throws Exception {
    expectNegativeValueOfResource("test1");
    parseResourceConfigValue("vcores=2,memory-mb=5120,test1=-4");
  }

  @Test
  public void testParseNewStyleResourceWithCustomResourceNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource("test1");
    parseResourceConfigValue(" vcores = 2 , memory-mb = 5120 , test1 = -4 ");
  }

  @Test
  public void testParseNewStyleResourceWithPercentagesVcoresNegative() throws
      Exception {
    expectNegativePercentageNewStyle();
    parseResourceConfigValue("vcores=-75%,memory-mb=40%");
  }

  @Test
  public void testParseNewStyleResourceWithPercentagesMemoryNegative() throws
      Exception {
    expectNegativePercentageNewStyle();
    parseResourceConfigValue("vcores=75%,memory-mb=-40%");
  }

  @Test
  public void testParseNewStyleResourceWithPercentagesVcoresNegativeWithSpaces()
      throws Exception {
    expectNegativePercentageNewStyle();
    parseResourceConfigValue("vcores=-75%, memory-mb=40%");
  }

  @Test
  public void testParseNewStyleResourceWithPercentagesMemoryNegativeWithSpaces()
      throws Exception {
    expectNegativePercentageNewStyle();
    parseResourceConfigValue("vcores=75%, memory-mb=-40%");
  }

  @Test
  public void
  testParseNewStyleResourceWithPercentagesVcoresNegativeWithMoreSpaces()
      throws Exception {
    expectNegativePercentageNewStyle();
    parseResourceConfigValue("vcores = -75%, memory-mb = 40%");
  }

  @Test
  public void
  testParseNewStyleResourceWithPercentagesMemoryNegativeWithMoreSpaces()
      throws Exception {
    expectNegativePercentageNewStyle();
    parseResourceConfigValue("vcores = 75%, memory-mb = -40%");
  }

  @Test
  public void
  testParseNewStyleResourceWithPercentagesCustomResourceNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource("test1");
    parseResourceConfigValue(" vcores = 2 , memory-mb = 5120 , test1 = -4 ");
  }

  @Test
  public void testAllocationIncrementMemoryDefaultUnit() {
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
  public void testAllocationIncrementCustomResource() {
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
          calculator.normalize(customResource(19999L, ""), min, max, increment)
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
    CustomResourceTypesConfigurationProvider.initResourceTypes(
        ImmutableMap.<String, String>builder()
            .put(A_CUSTOM_RESOURCE, "k")
            .build());
  }

  @Test
  public void testMemoryIncrementConfiguredViaMultipleProperties() {
    TestAppender testAppender = new TestAppender();
    Logger logger = LogManager.getRootLogger();
    logger.addAppender(testAppender);
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
      logger.removeAppender(testAppender);
    }
  }

  @Test
  public void testCpuIncrementConfiguredViaMultipleProperties() {
    TestAppender testAppender = new TestAppender();
    Logger logger = LogManager.getRootLogger();
    logger.addAppender(testAppender);
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
      logger.removeAppender(testAppender);
    }
  }
}

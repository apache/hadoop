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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration.parseResourceConfigValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  private void expect(String resource, Executable exec) {
    final Exception ex = Assertions.assertThrows(
        AllocationConfigurationException.class,
        exec
    );
    Assertions.assertTrue(
        ex.getMessage().contains(resource)
    );
  }

  private void expectMissingResource(String resource, Executable exec) {
    expect("Missing resource: " + resource, exec);
  }

  private void expectUnparsableResource(String resource, Executable exec) {
    expect("Cannot parse resource values from input: "
        + resource, exec);
  }

  private void expectInvalidResource(String resource, Executable exec) {
    expect("Invalid value of " + resource + ": ", exec);
  }

  private void expectInvalidResourcePercentage(String resource, Executable exec) {
    expect("Invalid percentage of " + resource + ": ", exec);
  }

  private void expectInvalidResourcePercentageNewStyle(String value, Executable exec) {
    expect("\"" + value + "\" is either " +
        "not a non-negative number", exec);
  }

  private void expectNegativePercentageOldStyle(Executable exec) {
    expect("percentage should not be negative", exec);
  }

  private void expectNegativePercentageNewStyle(Executable exec) {
    expect("is either not a non-negative number", exec);
  }

  private void expectNegativeValueOfResource(String resource, Executable exec) {
    expect("Invalid value of " + resource, exec);
  }

  @Test
  void testParseResourceConfigValue() throws Exception {
    Resource expected = BuilderUtils.newResource(5 * 1024, 2);
    Resource clusterResource = BuilderUtils.newResource(10 * 1024, 4);

    assertEquals(expected,
        parseResourceConfigValue("5120 mb 2 vcores").getResource());
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
  void testNoUnits() throws Exception {
    String value = "1024";
    expectUnparsableResource(
        value,
        () -> parseResourceConfigValue(value)
    );
  }

  @Test
  void testOnlyMemory() throws Exception {
    String value = "1024mb";
    expectUnparsableResource(
        value,
        () -> parseResourceConfigValue(value)
    );
  }

  @Test
  void testOnlyCPU() throws Exception {
    String value = "1024vcores";
    expectUnparsableResource(
        value,
        () -> parseResourceConfigValue(value)
    );
  }

  @Test
  void testGibberish() throws Exception {
    String value = "1o24vc0res";
    expectUnparsableResource(
        value,
        () -> parseResourceConfigValue(value)
    );
  }

  @Test
  void testNoUnitsPercentage() throws Exception {
    expectMissingResource(
        "cpu",
        () -> parseResourceConfigValue("95%, 50% memory")
    );
  }

  @Test
  void testInvalidNumPercentage() throws Exception {
    expectInvalidResourcePercentage(
        "cpu",
        () -> parseResourceConfigValue("95A% cpu, 50% memory")
    );
  }

  @Test
  void testCpuPercentageMemoryAbsolute() throws Exception {
    expectMissingResource(
        "memory",
        () -> parseResourceConfigValue("50% cpu, 1024 mb")
    );
  }

  @Test
  void testMemoryPercentageCpuAbsolute() throws Exception {
    expectMissingResource(
        "cpu",
        () -> parseResourceConfigValue("50% memory, 2 vcores")
    );
  }

  @Test
  void testDuplicateVcoresDefinitionAbsolute() throws Exception {
    expectInvalidResource(
        "vcores",
        () -> parseResourceConfigValue("1024 mb, 2 4 vcores")
    );

  }

  @Test
  void testDuplicateMemoryDefinitionAbsolute() throws Exception {
    expectInvalidResource(
        "memory",
        () -> parseResourceConfigValue("2048 1024 mb, 2 vcores")
    );
  }

  @Test
  void testDuplicateVcoresDefinitionPercentage() throws Exception {
    expectInvalidResourcePercentage(
        "cpu",
        () -> parseResourceConfigValue("50% memory, 50% 100%cpu")
    );
  }

  @Test
  void testDuplicateMemoryDefinitionPercentage() throws Exception {
    expectInvalidResourcePercentage(
        "memory",
        () -> parseResourceConfigValue("50% 80% memory, 100%cpu")
    );
  }

  @Test
  void testParseNewStyleDuplicateMemoryDefinitionPercentage()
      throws Exception {
    expectInvalidResourcePercentageNewStyle(
        "40% 80%",
        () -> parseResourceConfigValue("vcores = 75%, memory-mb = 40% 80%")
    );
  }

  @Test
  void testParseNewStyleDuplicateVcoresDefinitionPercentage()
      throws Exception {
    expectInvalidResourcePercentageNewStyle(
        "75% 65%",
        () -> parseResourceConfigValue("vcores = 75% 65%, memory-mb = 40%")
    );
  }

  @Test
  void testMemoryPercentageNegativeValue() throws Exception {
    expectNegativePercentageOldStyle(
        () -> parseResourceConfigValue("-10% memory, 50% cpu")
    );
  }

  @Test
  void testCpuPercentageNegativeValue() throws Exception {
    expectNegativePercentageOldStyle(
        () -> parseResourceConfigValue("10% memory, -10% cpu")
    );
  }

  @Test
  void testMemoryAndCpuPercentageNegativeValue() throws Exception {
    expectNegativePercentageOldStyle(
        () -> parseResourceConfigValue("-20% memory, -10% cpu")
    );
  }

  @Test
  void testCpuPercentageMemoryAbsoluteCpuNegative() throws Exception {
    expectMissingResource(
        "memory", () -> parseResourceConfigValue("-50% cpu, 1024 mb")
    );
  }

  @Test
  void testCpuPercentageMemoryAbsoluteMemoryNegative() throws Exception {
    expectMissingResource(
        "memory",
        () -> parseResourceConfigValue("50% cpu, -1024 mb")
    );
  }

  @Test
  void testMemoryPercentageCpuAbsoluteCpuNegative() throws Exception {
    expectMissingResource(
        "cpu",
        () -> parseResourceConfigValue("50% memory, -2 vcores")
    );
  }

  @Test
  void testMemoryPercentageCpuAbsoluteMemoryNegative() throws Exception {
    expectNegativePercentageOldStyle(
        () -> parseResourceConfigValue("-50% memory, 2 vcores")
    );
  }

  @Test
  void testAbsoluteVcoresNegative() throws Exception {
    expectNegativeValueOfResource(
        "vcores",
        () -> parseResourceConfigValue("-2 vcores,5120 mb")
    );
  }

  @Test
  void testAbsoluteMemoryNegative() throws Exception {
    expectNegativeValueOfResource(
        "memory",
        () -> parseResourceConfigValue("2 vcores,-5120 mb")
    );
  }

  @Test
  void testAbsoluteVcoresNegativeWithSpaces() throws Exception {
    expectNegativeValueOfResource(
        "vcores",
        () -> parseResourceConfigValue("-2 vcores, 5120 mb")
    );
  }

  @Test
  void testAbsoluteMemoryNegativeWithSpaces() throws Exception {
    expectNegativeValueOfResource(
        "memory",
        () -> parseResourceConfigValue("2 vcores, -5120 mb")
    );
  }

  @Test
  void testAbsoluteVcoresNegativeWithMoreSpaces() throws Exception {
    expectNegativeValueOfResource(
        "vcores",
        () -> parseResourceConfigValue("5120mb   mb, -2    vcores")
    );
  }

  @Test
  void testAbsoluteMemoryNegativeWithMoreSpaces() throws Exception {
    expectNegativeValueOfResource(
        "memory",
        () -> parseResourceConfigValue("-5120mb   mb, 2    vcores")
    );
  }

  @Test
  void testAbsoluteVcoresNegativeFractional() throws Exception {
    expectNegativeValueOfResource(
        "vcores",
        () -> parseResourceConfigValue("  5120.3 mb, -2.35 vcores  ")
    );
  }

  @Test
  void testAbsoluteMemoryNegativeFractional() throws Exception {
    expectNegativeValueOfResource(
        "memory",
        () -> parseResourceConfigValue("  -5120.3 mb, 2.35 vcores  ")
    );
  }

  @Test
  void testOldStyleResourcesSeparatedBySpaces() throws Exception {
    parseResourceConfigValue("2 vcores, 5120 mb");
  }

  @Test
  void testOldStyleResourcesSeparatedBySpacesInvalid() throws Exception {
    String value = "2 vcores 5120 mb 555 mb";
    expectUnparsableResource(
        value,
        () -> parseResourceConfigValue(value)
    );
  }

  @Test
  void testOldStyleResourcesSeparatedBySpacesInvalidUppercaseUnits()
      throws Exception {
    String value = "2 vcores 5120 MB 555 GB";
    expectUnparsableResource(
        value,
        () -> parseResourceConfigValue(value)
    );
  }

  @Test
  void testParseNewStyleResourceMemoryNegative() throws Exception {
    expectNegativeValueOfResource(
        "memory",
        () -> parseResourceConfigValue("memory-mb=-5120,vcores=2")
    );
  }

  @Test
  void testParseNewStyleResourceVcoresNegative() throws Exception {
    expectNegativeValueOfResource(
        "vcores",
        () -> parseResourceConfigValue("memory-mb=5120,vcores=-2")
    );
  }

  @Test
  void testParseNewStyleResourceMemoryNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource("memory", () ->
    parseResourceConfigValue("memory-mb=-5120, vcores=2"));
  }

  @Test
  void testParseNewStyleResourceVcoresNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource("vcores", () ->
    parseResourceConfigValue("memory-mb=5120, vcores=-2"));
  }

  @Test
  void testParseNewStyleResourceMemoryNegativeWithMoreSpaces()
      throws Exception {
    expectNegativeValueOfResource(
        "memory",
        () -> parseResourceConfigValue(" vcores = 2 ,  memory-mb = -5120 ")
    );
  }

  @Test
  void testParseNewStyleResourceVcoresNegativeWithMoreSpaces()
      throws Exception {
    expectNegativeValueOfResource(
        "vcores",
        () -> parseResourceConfigValue(" vcores = -2 ,  memory-mb = 5120 ")
    );
  }

  @Test
  void testParseNewStyleResourceWithCustomResourceMemoryNegative()
      throws Exception {
    expectNegativeValueOfResource(
        "memory",
        () -> parseResourceConfigValue("vcores=2,memory-mb=-5120,test1=4")
    );
  }

  @Test
  void testParseNewStyleResourceWithCustomResourceVcoresNegative()
      throws Exception {
    expectNegativeValueOfResource(
        "vcores",
        () -> parseResourceConfigValue("vcores=-2,memory-mb=-5120,test1=4")
    );
  }

  @Test
  void testParseNewStyleResourceWithCustomResourceNegative()
      throws Exception {
    expectNegativeValueOfResource(
        "test1",
        () -> parseResourceConfigValue("vcores=2,memory-mb=5120,test1=-4")
    );
  }

  @Test
  void testParseNewStyleResourceWithCustomResourceNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource(
        "test1",
        () -> parseResourceConfigValue(" vcores = 2 , memory-mb = 5120 , test1 = -4 ")
    );
  }

  @Test
  void testParseNewStyleResourceWithPercentagesVcoresNegative() throws
      Exception {
    expectNegativePercentageNewStyle(
        () -> parseResourceConfigValue("vcores=-75%,memory-mb=40%")
    );
  }

  @Test
  void testParseNewStyleResourceWithPercentagesMemoryNegative() throws
      Exception {
    expectNegativePercentageNewStyle(
        () -> parseResourceConfigValue("vcores=75%,memory-mb=-40%")
    );
  }

  @Test
  void testParseNewStyleResourceWithPercentagesVcoresNegativeWithSpaces()
      throws Exception {
    expectNegativePercentageNewStyle(
        () -> parseResourceConfigValue("vcores=-75%, memory-mb=40%")
    );
  }

  @Test
  void testParseNewStyleResourceWithPercentagesMemoryNegativeWithSpaces()
      throws Exception {
    expectNegativePercentageNewStyle(
        () -> parseResourceConfigValue("vcores=75%, memory-mb=-40%")
    );
  }

  @Test
  void
  testParseNewStyleResourceWithPercentagesVcoresNegativeWithMoreSpaces()
      throws Exception {
    expectNegativePercentageNewStyle(
        () -> parseResourceConfigValue("vcores = -75%, memory-mb = 40%")
    );
  }

  @Test
  void
  testParseNewStyleResourceWithPercentagesMemoryNegativeWithMoreSpaces()
      throws Exception {
    expectNegativePercentageNewStyle(
        () -> parseResourceConfigValue("vcores = 75%, memory-mb = -40%")
    );
  }

  @Test
  void
  testParseNewStyleResourceWithPercentagesCustomResourceNegativeWithSpaces()
      throws Exception {
    expectNegativeValueOfResource(
        "test1",
        () -> parseResourceConfigValue(" vcores = 2 , memory-mb = 5120 , test1 = -4 ")
    );
  }

  @Test
  void testAllocationIncrementMemoryDefaultUnit() {
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
  void testAllocationIncrementMemoryNonDefaultUnit() throws Exception {
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

  @Test
  void testAllocationIncrementInvalidUnit() throws Exception {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      Configuration conf = new Configuration();
      conf.set(YarnConfiguration.RESOURCE_TYPES + "." +
          ResourceInformation.MEMORY_MB.getName() +
          FairSchedulerConfiguration.INCREMENT_ALLOCATION, "1 Xi");
      new FairSchedulerConfiguration(conf).getIncrementAllocation();
    });
  }

  @Test
  void testAllocationIncrementVCoreNoUnit() throws Exception {
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
  void testAllocationIncrementVCoreWithUnit() throws Exception {
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
  void testAllocationIncrementCustomResource() {
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
  void testMemoryIncrementConfiguredViaMultipleProperties() {
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
      Assertions.assertEquals(13L, increment.getMemorySize());
      assertTrue(testAppender.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && ("Configuration " +
              "yarn.resource-types.memory-mb.increment-allocation=13 is " +
              "overriding the yarn.scheduler.increment-allocation-mb=7 " +
              "property").equals(e.getMessage())),
          "Warning message is not logged when specifying memory " +
          "increment via multiple properties");
    } finally {
      logger.removeAppender(testAppender);
    }
  }

  @Test
  void testCpuIncrementConfiguredViaMultipleProperties() {
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
      Assertions.assertEquals(13, increment.getVirtualCores());
      assertTrue(testAppender.getLogEvents().stream().anyMatch(
            e -> e.getLevel() == Level.WARN && ("Configuration " +
              "yarn.resource-types.vcores.increment-allocation=13 is " +
              "overriding the yarn.scheduler.increment-allocation-vcores=7 " +
              "property").equals(e.getMessage())),
          "Warning message is not logged when specifying CPU vCores " +
          "increment via multiple properties");
    } finally {
      logger.removeAppender(testAppender);
    }
  }
}

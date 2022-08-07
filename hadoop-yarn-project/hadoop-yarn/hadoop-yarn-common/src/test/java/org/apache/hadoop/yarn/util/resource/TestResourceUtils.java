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

package org.apache.hadoop.yarn.util.resource;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class to verify all resource utility methods.
 */
public class TestResourceUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestResourceUtils.class);

  private File nodeResourcesFile;
  private File resourceTypesFile;

  static class ResourceFileInformation {
    String filename;
    int resourceCount;
    Map<String, String> resourceNameUnitsMap;

    ResourceFileInformation(String name, int count) {
      filename = name;
      resourceCount = count;
      resourceNameUnitsMap = new HashMap<>();
    }
  }

  @BeforeEach
  public void setup() {
    ResourceUtils.resetResourceTypes();
  }

  @AfterEach
  public void teardown() {
    if (nodeResourcesFile != null && nodeResourcesFile.exists()) {
      nodeResourcesFile.delete();
    }
    if (resourceTypesFile != null && resourceTypesFile.exists()) {
      resourceTypesFile.delete();
    }
  }

  public static String setupResourceTypes(Configuration conf, String filename)
      throws Exception {
    File source = new File(
        conf.getClassLoader().getResource(filename).getFile());
    File dest = new File(source.getParent(), "resource-types.xml");
    FileUtils.copyFile(source, dest);
    try {
      ResourceUtils.getResourceTypes();
    } catch (Exception e) {
      if (!dest.delete()) {
        LOG.error("Could not delete {}", dest);
      }
      throw e;
    }
    return dest.getAbsolutePath();
  }

  private Map<String, ResourceInformation> setupResourceTypesInternal(
      Configuration conf, String srcFileName) throws IOException {
    URL srcFileUrl = conf.getClassLoader().getResource(srcFileName);
    if (srcFileUrl == null) {
      throw new IllegalArgumentException(
          "Source file does not exist: " + srcFileName);
    }
    File source = new File(srcFileUrl.getFile());
    File dest = new File(source.getParent(), "resource-types.xml");
    FileUtils.copyFile(source, dest);
    this.resourceTypesFile = dest;
    return ResourceUtils.getResourceTypes();
  }

  private Map<String, ResourceInformation> setupNodeResources(
      Configuration conf, String srcFileName) throws IOException {
    URL srcFileUrl = conf.getClassLoader().getResource(srcFileName);
    if (srcFileUrl == null) {
      throw new IllegalArgumentException(
          "Source file does not exist: " + srcFileName);
    }
    File source = new File(srcFileUrl.getFile());
    File dest = new File(source.getParent(), "node-resources.xml");
    FileUtils.copyFile(source, dest);
    this.nodeResourcesFile = dest;
    return ResourceUtils
        .getNodeResourceInformation(conf);
  }

  private void testMemoryAndVcores(Map<String, ResourceInformation> res) {
    String memory = ResourceInformation.MEMORY_MB.getName();
    String vcores = ResourceInformation.VCORES.getName();
    assertTrue(res.containsKey(memory), "Resource 'memory' missing");
    assertEquals(ResourceInformation.MEMORY_MB.getUnits(), res.get(memory).getUnits(),
        "'memory' units incorrect");
    assertEquals(ResourceInformation.MEMORY_MB.getResourceType(), res.get(memory).getResourceType(),
        "'memory' types incorrect");
    assertTrue(res.containsKey(vcores), "Resource 'vcores' missing");
    assertEquals(ResourceInformation.VCORES.getUnits(), res.get(vcores).getUnits(),
        "'vcores' units incorrect");
    assertEquals(ResourceInformation.VCORES.getResourceType(),
        res.get(vcores).getResourceType(),
        "'vcores' type incorrect");
  }

  @Test
  void testGetResourceTypes() {
    Map<String, ResourceInformation> res = ResourceUtils.getResourceTypes();
    assertEquals(2, res.size());
    testMemoryAndVcores(res);
  }

  @Test
  void testGetResourceTypesConfigs() throws Exception {
    Configuration conf = new YarnConfiguration();

    ResourceFileInformation testFile1 =
        new ResourceFileInformation("resource-types-1.xml", 2);
    ResourceFileInformation testFile2 =
        new ResourceFileInformation("resource-types-2.xml", 3);
    testFile2.resourceNameUnitsMap.put("resource1", "G");
    ResourceFileInformation testFile3 =
        new ResourceFileInformation("resource-types-3.xml", 3);
    testFile3.resourceNameUnitsMap.put("resource2", "");
    ResourceFileInformation testFile4 =
        new ResourceFileInformation("resource-types-4.xml", 5);
    testFile4.resourceNameUnitsMap.put("resource1", "G");
    testFile4.resourceNameUnitsMap.put("resource2", "m");
    testFile4.resourceNameUnitsMap.put("yarn.io/gpu", "");

    ResourceFileInformation[] tests = {testFile1, testFile2, testFile3,
        testFile4};
    Map<String, ResourceInformation> res;
    for (ResourceFileInformation testInformation : tests) {
      ResourceUtils.resetResourceTypes();
      res = setupResourceTypesInternal(conf, testInformation.filename);
      testMemoryAndVcores(res);
      assertEquals(testInformation.resourceCount, res.size());
      for (Map.Entry<String, String> entry :
          testInformation.resourceNameUnitsMap.entrySet()) {
        String resourceName = entry.getKey();
        assertTrue(res.containsKey(resourceName),
            "Missing key " + resourceName);
        assertEquals(entry.getValue(), res.get(resourceName).getUnits());
      }
    }
  }

  @Test
  void testGetRequestedResourcesFromConfig() {
    Configuration conf = new Configuration();

    //these resource type configurations should be recognised
    String propertyPrefix = "mapreduce.mapper.proper.rt.";
    String[] expectedKeys = {
        "yarn.io/gpu",
        "yarn.io/fpga",
        "yarn.io/anything_without_a_dot",
        "regular_rt",
        "regular_rt/with_slash"};

    String[] invalidKeys = {
        propertyPrefix + "too.many_parts",
        propertyPrefix + "yarn.notio/gpu",
        "incorrect.prefix.yarn.io/gpu",
        propertyPrefix + "yarn.io/",
        propertyPrefix};

    for (String s : expectedKeys) {
      //setting the properties which are expected to be in the resource list
      conf.set(propertyPrefix + s, "42");
    }

    for (String s : invalidKeys) {
      //setting the properties which are expected to be in the resource list
      conf.set(s, "24");
    }

    List<ResourceInformation> properList =
        ResourceUtils.getRequestedResourcesFromConfig(conf, propertyPrefix);
    Set<String> expectedSet =
        new HashSet<>(Arrays.asList(expectedKeys));

    assertEquals(properList.size(), expectedKeys.length);
    properList.forEach(
        item -> assertTrue(expectedSet.contains(item.getName())));

  }

  @Test
  void testGetResourceTypesConfigErrors() throws IOException {
    Configuration conf = new YarnConfiguration();

    String[] resourceFiles = {"resource-types-error-1.xml",
        "resource-types-error-2.xml", "resource-types-error-3.xml",
        "resource-types-error-4.xml"};
    for (String resourceFile : resourceFiles) {
      ResourceUtils.resetResourceTypes();
      try {
        setupResourceTypesInternal(conf, resourceFile);
        fail("Expected error with file " + resourceFile);
      } catch (YarnRuntimeException | IllegalArgumentException e) {
        //Test passed
      }
    }
  }

  @Test
  void testInitializeResourcesMap() {
    String[] empty = {"", ""};
    String[] res1 = {"resource1", "m"};
    String[] res2 = {"resource2", "G"};
    String[][] test1 = {empty};
    String[][] test2 = {res1};
    String[][] test3 = {res2};
    String[][] test4 = {res1, res2};

    String[][][] allTests = {test1, test2, test3, test4};

    for (String[][] test : allTests) {

      Configuration conf = new YarnConfiguration();
      String resSt = "";
      for (String[] resources : test) {
        resSt += (resources[0] + ",");
      }
      resSt = resSt.substring(0, resSt.length() - 1);
      conf.set(YarnConfiguration.RESOURCE_TYPES, resSt);
      for (String[] resources : test) {
        String name =
            YarnConfiguration.RESOURCE_TYPES + "." + resources[0] + ".units";
        conf.set(name, resources[1]);
      }
      Map<String, ResourceInformation> ret =
          ResourceUtils.resetResourceTypes(conf);

      // for test1, 4 - length will be 1, 4
      // for the others, len will be 3
      int len = 3;
      if (test == test1) {
        len = 2;
      } else if (test == test4) {
        len = 4;
      }

      assertEquals(len, ret.size());
      for (String[] resources : test) {
        if (resources[0].length() == 0) {
          continue;
        }
        assertTrue(ret.containsKey(resources[0]));
        ResourceInformation resInfo = ret.get(resources[0]);
        assertEquals(resources[1], resInfo.getUnits());
        assertEquals(ResourceTypes.COUNTABLE, resInfo.getResourceType());
      }
      // we must always have memory and vcores with their fixed units
      assertTrue(ret.containsKey("memory-mb"));
      ResourceInformation memInfo = ret.get("memory-mb");
      assertEquals("Mi", memInfo.getUnits());
      assertEquals(ResourceTypes.COUNTABLE, memInfo.getResourceType());
      assertTrue(ret.containsKey("vcores"));
      ResourceInformation vcoresInfo = ret.get("vcores");
      assertEquals("", vcoresInfo.getUnits());
      assertEquals(ResourceTypes.COUNTABLE, vcoresInfo.getResourceType());
    }
  }

  @Test
  void testInitializeResourcesMapErrors() {
    String[] mem1 = {"memory-mb", ""};
    String[] vcores1 = {"vcores", "M"};

    String[] mem2 = {"memory-mb", "m"};
    String[] vcores2 = {"vcores", "G"};

    String[] mem3 = {"memory", ""};

    String[][] test1 = {mem1, vcores1};
    String[][] test2 = {mem2, vcores2};
    String[][] test3 = {mem3};

    String[][][] allTests = {test1, test2, test3};

    for (String[][] test : allTests) {

      Configuration conf = new YarnConfiguration();
      String resSt = "";
      for (String[] resources : test) {
        resSt += (resources[0] + ",");
      }
      resSt = resSt.substring(0, resSt.length() - 1);
      conf.set(YarnConfiguration.RESOURCE_TYPES, resSt);
      for (String[] resources : test) {
        String name =
            YarnConfiguration.RESOURCE_TYPES + "." + resources[0] + ".units";
        conf.set(name, resources[1]);
      }
      try {
        ResourceUtils.initializeResourcesMap(conf);
        fail("resource map initialization should fail");
      } catch (Exception e) {
        //Test passed
      }
    }
  }

  @Test
  void testGetResourceInformation() throws Exception {
    Configuration conf = new YarnConfiguration();
    Map<String, Resource> testRun = new HashMap<>();
    setupResourceTypesInternal(conf, "resource-types-4.xml");
    Resource test3Resources = Resource.newInstance(0, 0);
    test3Resources.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "Gi", 5L));
    test3Resources.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "m", 2L));
    test3Resources.setResourceInformation("yarn.io/gpu",
        ResourceInformation.newInstance("yarn.io/gpu", "", 1));
    testRun.put("node-resources-2.xml", test3Resources);

    for (Map.Entry<String, Resource> entry : testRun.entrySet()) {
      String resourceFile = entry.getKey();
      ResourceUtils.resetNodeResources();
      Map<String, ResourceInformation> actual = setupNodeResources(conf,
          resourceFile);
      assertEquals(actual.size(),
          entry.getValue().getResources().length);
      for (ResourceInformation resInfo : entry.getValue().getResources()) {
        assertEquals(resInfo, actual.get(resInfo.getName()));
      }
    }
  }

  @Test
  void testGetNodeResourcesConfigErrors() throws Exception {
    Configuration conf = new YarnConfiguration();
    setupResourceTypesInternal(conf, "resource-types-4.xml");
    String[] invalidNodeResFiles = {"node-resources-error-1.xml"};

    for (String resourceFile : invalidNodeResFiles) {
      ResourceUtils.resetNodeResources();
      try {
        setupNodeResources(conf, resourceFile);
        fail("Expected error with file " + resourceFile);
      } catch (YarnRuntimeException e) {
        //Test passed
      }
    }
  }

  @Test
  void testGetNodeResourcesRedefineFpgaErrors() throws Exception {
    Throwable exception = assertThrows(YarnRuntimeException.class, () -> {
      Configuration conf = new YarnConfiguration();
      setupResourceTypesInternal(conf,
          "resource-types-error-redefine-fpga-unit.xml");
    });
    assertTrue(exception.getMessage().contains("Defined mandatory resource type=yarn.io/fpga"));
  }

  @Test
  void testGetNodeResourcesRedefineGpuErrors() throws Exception {
    Throwable exception = assertThrows(YarnRuntimeException.class, () -> {
      Configuration conf = new YarnConfiguration();
      setupResourceTypesInternal(conf,
          "resource-types-error-redefine-gpu-unit.xml");
    });
    assertTrue(exception.getMessage().contains("Defined mandatory resource type=yarn.io/gpu"));
  }

  @Test
  void testResourceNameFormatValidation() {
    String[] validNames = new String[]{
        "yarn.io/gpu",
        "gpu",
        "g_1_2",
        "123.io/gpu",
        "prefix/resource_1",
        "a___-3",
        "a....b",
    };

    String[] invalidNames = new String[]{
        "asd/resource/-name",
        "prefix/-resource_1",
        "prefix/0123resource",
        "0123resource",
        "-resource_1",
        "........abc"
    };

    for (String validName : validNames) {
      ResourceUtils.validateNameOfResourceNameAndThrowException(validName);
    }

    for (String invalidName : invalidNames) {
      try {
        ResourceUtils.validateNameOfResourceNameAndThrowException(invalidName);
        fail("Expected to fail name check, the name=" + invalidName
            + " is illegal.");
      } catch (YarnRuntimeException e) {
        // Expected
      }
    }
  }

  @Test
  void testGetResourceInformationWithDiffUnits() throws Exception {
    Configuration conf = new YarnConfiguration();
    Map<String, Resource> testRun = new HashMap<>();
    setupResourceTypesInternal(conf, "resource-types-4.xml");
    Resource test3Resources = Resource.newInstance(0, 0);

    //Resource 'resource1' has been passed as 5T
    //5T should be converted to 5000G
    test3Resources.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "T", 5L));

    //Resource 'resource2' has been passed as 2M
    //2M should be converted to 2000000000m
    test3Resources.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "M", 2L));
    test3Resources.setResourceInformation("yarn.io/gpu",
        ResourceInformation.newInstance("yarn.io/gpu", "", 1));
    testRun.put("node-resources-3.xml", test3Resources);

    for (Map.Entry<String, Resource> entry : testRun.entrySet()) {
      String resourceFile = entry.getKey();
      ResourceUtils.resetNodeResources();
      Map<String, ResourceInformation> actual = setupNodeResources(conf,
          resourceFile);
      assertEquals(actual.size(),
          entry.getValue().getResources().length);
      for (ResourceInformation resInfo : entry.getValue().getResources()) {
        assertEquals(resInfo, actual.get(resInfo.getName()));
      }
    }
  }

  @Test
  void testResourceUnitParsing() throws Exception {
    Resource res = ResourceUtils.createResourceFromString("memory=20g,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(20 * 1024, 3), res);

    res = ResourceUtils.createResourceFromString("memory=20G,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(20 * 1024, 3), res);

    res = ResourceUtils.createResourceFromString("memory=20M,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(20, 3), res);

    res = ResourceUtils.createResourceFromString("memory=20m,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(20, 3), res);

    res = ResourceUtils.createResourceFromString("memory-mb=20,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(20, 3), res);

    res = ResourceUtils.createResourceFromString("memory-mb=20m,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(20, 3), res);

    res = ResourceUtils.createResourceFromString("memory-mb=20G,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(20 * 1024, 3), res);

    // W/o unit for memory means bits, and 20 bits will be rounded to 0
    res = ResourceUtils.createResourceFromString("memory=20,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    assertEquals(Resources.createResource(0, 3), res);

    // Test multiple resources
    List<ResourceTypeInfo> resTypes = new ArrayList<>(
        ResourceUtils.getResourcesTypeInfo());
    resTypes.add(ResourceTypeInfo.newInstance(ResourceInformation.GPU_URI, ""));
    ResourceUtils.reinitializeResources(resTypes);
    res = ResourceUtils.createResourceFromString("memory=2G,vcores=3,gpu=0",
        resTypes);
    assertEquals(2 * 1024, res.getMemorySize());
    assertEquals(0, res.getResourceValue(ResourceInformation.GPU_URI));

    res = ResourceUtils.createResourceFromString("memory=2G,vcores=3,gpu=3",
        resTypes);
    assertEquals(2 * 1024, res.getMemorySize());
    assertEquals(3, res.getResourceValue(ResourceInformation.GPU_URI));

    res = ResourceUtils.createResourceFromString("memory=2G,vcores=3",
        resTypes);
    assertEquals(2 * 1024, res.getMemorySize());
    assertEquals(0, res.getResourceValue(ResourceInformation.GPU_URI));

    res = ResourceUtils.createResourceFromString(
        "memory=2G,vcores=3,yarn.io/gpu=0", resTypes);
    assertEquals(2 * 1024, res.getMemorySize());
    assertEquals(0, res.getResourceValue(ResourceInformation.GPU_URI));

    res = ResourceUtils.createResourceFromString(
        "memory=2G,vcores=3,yarn.io/gpu=3", resTypes);
    assertEquals(2 * 1024, res.getMemorySize());
    assertEquals(3, res.getResourceValue(ResourceInformation.GPU_URI));
  }

  @Test
  void testMultipleOpsForResourcesWithTags() throws Exception {

    Configuration conf = new YarnConfiguration();
    setupResourceTypes(conf, "resource-types-6.xml");
    Resource resourceA = Resource.newInstance(2, 4);
    Resource resourceB = Resource.newInstance(3, 6);

    resourceA.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "T", 5L));

    resourceA.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "M", 2L));
    resourceA.setResourceInformation("yarn.io/gpu",
        ResourceInformation.newInstance("yarn.io/gpu", "", 1));
    resourceA.setResourceInformation("yarn.io/test-volume",
        ResourceInformation.newInstance("yarn.io/test-volume", "", 2));

    resourceB.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "T", 3L));

    resourceB.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "M", 4L));
    resourceB.setResourceInformation("yarn.io/gpu",
        ResourceInformation.newInstance("yarn.io/gpu", "", 2));
    resourceB.setResourceInformation("yarn.io/test-volume",
        ResourceInformation.newInstance("yarn.io/test-volume", "", 3));

    Resource addedResource = Resources.add(resourceA, resourceB);
    assertThat(addedResource.getMemorySize()).isEqualTo(5);
    assertThat(addedResource.getVirtualCores()).isEqualTo(10);
    assertThat(addedResource.getResourceInformation("resource1").getValue()).
        isEqualTo(8);

    // Verify that value of resourceA and resourceB is not added up for
    // "yarn.io/test-volume".
    assertThat(addedResource.getResourceInformation("yarn.io/test-volume").
        getValue()).isEqualTo(2);

    Resource mulResource = Resources.multiplyAndRoundDown(resourceA, 3);
    assertThat(mulResource.getMemorySize()).isEqualTo(6);
    assertThat(mulResource.getVirtualCores()).isEqualTo(12);
    assertThat(mulResource.getResourceInformation("resource1").getValue()).
        isEqualTo(15);

    // Verify that value of resourceA is not multiplied up for
    // "yarn.io/test-volume".
    assertThat(mulResource.getResourceInformation("yarn.io/test-volume").
        getValue()).isEqualTo(2);
  }
}

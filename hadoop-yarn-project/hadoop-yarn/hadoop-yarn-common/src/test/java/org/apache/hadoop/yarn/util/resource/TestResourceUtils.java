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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class TestResourceUtils {

  static class ResourceFileInformation {
    String filename;
    int resourceCount;
    Map<String, String> resourceNameUnitsMap;

    public ResourceFileInformation(String name, int count) {
      filename = name;
      resourceCount = count;
      resourceNameUnitsMap = new HashMap<>();
    }
  }

  @Before
  public void setup() {
    ResourceUtils.resetResourceTypes();
  }

  @After
  public void teardown() {
    Configuration conf = new YarnConfiguration();
    File source = new File(
        conf.getClassLoader().getResource("resource-types-1.xml").getFile());
    File dest = new File(source.getParent(), "resource-types.xml");
    if (dest.exists()) {
      dest.delete();
    }
  }

  private void testMemoryAndVcores(Map<String, ResourceInformation> res) {
    String memory = ResourceInformation.MEMORY_MB.getName();
    String vcores = ResourceInformation.VCORES.getName();
    Assert.assertTrue("Resource 'memory' missing", res.containsKey(memory));
    Assert.assertEquals("'memory' units incorrect",
        ResourceInformation.MEMORY_MB.getUnits(), res.get(memory).getUnits());
    Assert.assertEquals("'memory' types incorrect",
        ResourceInformation.MEMORY_MB.getResourceType(),
        res.get(memory).getResourceType());
    Assert.assertTrue("Resource 'vcores' missing", res.containsKey(vcores));
    Assert.assertEquals("'vcores' units incorrect",
        ResourceInformation.VCORES.getUnits(), res.get(vcores).getUnits());
    Assert.assertEquals("'vcores' type incorrect",
        ResourceInformation.VCORES.getResourceType(),
        res.get(vcores).getResourceType());
  }

  @Test
  public void testGetResourceTypes() throws Exception {

    Map<String, ResourceInformation> res = ResourceUtils.getResourceTypes();
    Assert.assertEquals(2, res.size());
    testMemoryAndVcores(res);
  }

  @Test
  public void testGetResourceTypesConfigs() throws Exception {

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
        new ResourceFileInformation("resource-types-4.xml", 4);
    testFile4.resourceNameUnitsMap.put("resource1", "G");
    testFile4.resourceNameUnitsMap.put("resource2", "m");

    ResourceFileInformation[] tests =
        { testFile1, testFile2, testFile3, testFile4 };
    Map<String, ResourceInformation> res;
    for (ResourceFileInformation testInformation : tests) {
      ResourceUtils.resetResourceTypes();
      File source = new File(
          conf.getClassLoader().getResource(testInformation.filename)
              .getFile());
      File dest = new File(source.getParent(), "resource-types.xml");
      FileUtils.copyFile(source, dest);
      res = ResourceUtils.getResourceTypes();
      testMemoryAndVcores(res);
      Assert.assertEquals(testInformation.resourceCount, res.size());
      for (Map.Entry<String, String> entry : testInformation.resourceNameUnitsMap
          .entrySet()) {
        String resourceName = entry.getKey();
        Assert.assertTrue("Missing key " + resourceName,
            res.containsKey(resourceName));
        Assert.assertEquals(entry.getValue(), res.get(resourceName).getUnits());
      }
      dest.delete();
    }
  }

  @Test
  public void testGetResourceTypesConfigErrors() throws Exception {
    Configuration conf = new YarnConfiguration();

    String[] resourceFiles =
        { "resource-types-error-1.xml", "resource-types-error-2.xml",
            "resource-types-error-3.xml", "resource-types-error-4.xml" };
    for (String resourceFile : resourceFiles) {
      ResourceUtils.resetResourceTypes();
      File dest = null;
      try {
        File source =
            new File(conf.getClassLoader().getResource(resourceFile).getFile());
        dest = new File(source.getParent(), "resource-types.xml");
        FileUtils.copyFile(source, dest);
        ResourceUtils.getResourceTypes();
        Assert.fail("Expected error with file " + resourceFile);
      } catch (NullPointerException ne) {
        throw ne;
      } catch (Exception e) {
        if (dest != null) {
          dest.delete();
        }
      }
    }
  }

  @Test
  public void testInitializeResourcesMap() throws Exception {
    String[] empty = { "", "" };
    String[] res1 = { "resource1", "m" };
    String[] res2 = { "resource2", "G" };
    String[][] test1 = { empty };
    String[][] test2 = { res1 };
    String[][] test3 = { res2 };
    String[][] test4 = { res1, res2 };

    String[][][] allTests = { test1, test2, test3, test4 };

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
      Map<String, ResourceInformation> ret = new HashMap<>();
      ResourceUtils.initializeResourcesMap(conf, ret);
      // for test1, 4 - length will be 1, 4
      // for the others, len will be 3
      int len = 3;
      if (test == test1) {
        len = 2;
      } else if (test == test4) {
        len = 4;
      }

      Assert.assertEquals(len, ret.size());
      for (String[] resources : test) {
        if (resources[0].length() == 0) {
          continue;
        }
        Assert.assertTrue(ret.containsKey(resources[0]));
        ResourceInformation resInfo = ret.get(resources[0]);
        Assert.assertEquals(resources[1], resInfo.getUnits());
        Assert.assertEquals(ResourceTypes.COUNTABLE, resInfo.getResourceType());
      }
      // we must always have memory and vcores with their fixed units
      Assert.assertTrue(ret.containsKey("memory-mb"));
      ResourceInformation memInfo = ret.get("memory-mb");
      Assert.assertEquals("Mi", memInfo.getUnits());
      Assert.assertEquals(ResourceTypes.COUNTABLE, memInfo.getResourceType());
      Assert.assertTrue(ret.containsKey("vcores"));
      ResourceInformation vcoresInfo = ret.get("vcores");
      Assert.assertEquals("", vcoresInfo.getUnits());
      Assert
          .assertEquals(ResourceTypes.COUNTABLE, vcoresInfo.getResourceType());
    }
  }

  @Test
  public void testInitializeResourcesMapErrors() throws Exception {

    String[] mem1 = { "memory-mb", "" };
    String[] vcores1 = { "vcores", "M" };

    String[] mem2 = { "memory-mb", "m" };
    String[] vcores2 = { "vcores", "G" };

    String[] mem3 = { "memory", "" };

    String[][] test1 = { mem1, vcores1 };
    String[][] test2 = { mem2, vcores2 };
    String[][] test3 = { mem3 };

    String[][][] allTests = { test1, test2, test3 };

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
      Map<String, ResourceInformation> ret = new HashMap<>();
      try {
        ResourceUtils.initializeResourcesMap(conf, ret);
        Assert.fail("resource map initialization should fail");
      } catch (Exception e) {
        // do nothing
      }
    }
  }

  @Test
  public void testGetResourceInformation() throws Exception {

    Configuration conf = new YarnConfiguration();
    Map<String, Resource> testRun = new HashMap<>();
    setupResourceTypes(conf, "resource-types-4.xml");
    // testRun.put("node-resources-1.xml", Resource.newInstance(1024, 1));
    Resource test3Resources = Resource.newInstance(1024, 1);
    test3Resources.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "Gi", 5L));
    test3Resources.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "m", 2L));
    testRun.put("node-resources-2.xml", test3Resources);

    for (Map.Entry<String, Resource> entry : testRun.entrySet()) {
      String resourceFile = entry.getKey();
      ResourceUtils.resetNodeResources();
      File dest;
      File source =
          new File(conf.getClassLoader().getResource(resourceFile).getFile());
      dest = new File(source.getParent(), "node-resources.xml");
      FileUtils.copyFile(source, dest);
      Map<String, ResourceInformation> actual =
          ResourceUtils.getNodeResourceInformation(conf);
      Assert.assertEquals(entry.getValue().getResources(), actual);
    }
  }

  public static String setupResourceTypes(Configuration conf, String filename)
      throws Exception {
    File source = new File(
        conf.getClassLoader().getResource(filename).getFile());
    File dest = new File(source.getParent(), "resource-types.xml");
    FileUtils.copyFile(source, dest);
    ResourceUtils.getResourceTypes();
    return dest.getAbsolutePath();
  }
}

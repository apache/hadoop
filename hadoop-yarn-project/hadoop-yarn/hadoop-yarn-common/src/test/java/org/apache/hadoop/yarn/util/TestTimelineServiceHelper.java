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
package org.apache.hadoop.yarn.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestTimelineServiceHelper {

  @AfterEach
  void resetResourceTypes() {
    ResourceUtils.resetResourceTypes(new YarnConfiguration());
  }

  @Test
  void testContainerResourceRoundTrip() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RESOURCE_TYPES,
        "yarn.io/gpu,example.com/bandwidth,example.com/unused");
    conf.set("yarn.resource-types.example.com/bandwidth.units", "M");
    ResourceUtils.resetResourceTypes(conf);
    Resource allocated = Resource.newInstance(Integer.MAX_VALUE + 1L, 4);
    allocated.setResourceValue("yarn.io/gpu", 2);
    allocated.getResourceInformation("example.com/bandwidth").setUnits("G");
    allocated.setResourceValue("example.com/bandwidth", Integer.MAX_VALUE + 1L);
    Map<String, Object> info = new HashMap<>();
    info.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO,
        allocated.getMemorySize());
    info.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO, 4);
    info.put(ContainerMetricsConstants.ALLOCATED_RESOURCES_INFO,
        TimelineUtils.getCustomResourceInfo(allocated));
    allocated.setResourceValue("yarn.io/gpu", 0);

    Map<String, Object> stored = new ObjectMapper().readValue(
        TimelineUtils.dumpTimelineRecordtoJSON(info),
        new TypeReference<Map<String, Object>>() { });
    Map<?, ?> custom = (Map<?, ?>) stored.get(
        ContainerMetricsConstants.ALLOCATED_RESOURCES_INFO);
    assertEquals(2, custom.size());
    assertEquals(2, ((Map<?, ?>) custom.get("yarn.io/gpu")).get("value"));
    assertThat(custom.containsKey("example.com/unused")).isFalse();
    Resource restored = TimelineUtils.getContainerResource(stored);
    assertEquals(Integer.MAX_VALUE + 1L, restored.getMemorySize());
    assertEquals(4, restored.getVirtualCores());
    assertEquals(2, restored.getResourceValue("yarn.io/gpu"));
    assertEquals((Integer.MAX_VALUE + 1L) * 1000,
        restored.getResourceValue("example.com/bandwidth"));
    assertEquals("M",
        restored.getResourceInformation("example.com/bandwidth").getUnits());
    assertEquals(0, restored.getResourceValue("example.com/unused"));

    ResourceUtils.resetResourceTypes(new YarnConfiguration());
    Resource withoutCustomTypes = TimelineUtils.getContainerResource(stored);
    assertEquals(Integer.MAX_VALUE + 1L, withoutCustomTypes.getMemorySize());
    assertEquals(4, withoutCustomTypes.getVirtualCores());
    assertEquals(2, withoutCustomTypes.getResources().length);
    assertEquals(2, custom.size());
  }

  @Test
  void testLegacyContainerResources() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RESOURCE_TYPES, "yarn.io/gpu");
    ResourceUtils.resetResourceTypes(conf);
    Map<String, Object> info = new HashMap<>();
    info.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO, 1024);
    info.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO, 2);
    Resource restored = TimelineUtils.getContainerResource(info);
    assertEquals(1024, restored.getMemorySize());
    assertEquals(2, restored.getVirtualCores());
    assertEquals(0, restored.getResourceValue("yarn.io/gpu"));
    assertEquals(Resource.newInstance(0, 0),
        TimelineUtils.getContainerResource(null));
    assertEquals(Resource.newInstance(0, 0),
        TimelineUtils.getContainerResource(new HashMap<>()));
  }

  @Test
  void testMalformedContainerResources() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RESOURCE_TYPES,
        "yarn.io/gpu,example.com/fpga,example.com/bandwidth,"
            + "example.com/overflow,example.com/invalid,"
            + "example.com/oversized,example.com/fractional,"
            + "example.com/rounded");
    conf.set("yarn.resource-types.example.com/overflow.units", "M");
    ResourceUtils.resetResourceTypes(conf);
    Map<String, Object> info = new HashMap<>();
    info.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO, 1024);
    info.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO, 2);
    Map<String, Object> allocations = new LinkedHashMap<>();
    Map<String, Object> missingValue = new HashMap<>();
    missingValue.put("units", "");
    allocations.put("example.com/fpga", missingValue);
    Map<String, Object> unknownUnit = new HashMap<>();
    unknownUnit.put("value", 3);
    unknownUnit.put("units", "unknown");
    allocations.put("example.com/bandwidth", unknownUnit);
    Map<String, Object> overflow = new HashMap<>();
    overflow.put("value", Long.MAX_VALUE);
    overflow.put("units", "G");
    allocations.put("example.com/overflow", overflow);
    allocations.put("example.com/invalid", "invalid");
    Map<String, Object> oversized = new HashMap<>();
    oversized.put("value", new BigInteger("9223372036854775808"));
    oversized.put("units", "");
    allocations.put("example.com/oversized", oversized);
    Map<String, Object> fractional = new HashMap<>();
    fractional.put("value", new BigDecimal("2.75"));
    fractional.put("units", "");
    allocations.put("example.com/fractional", fractional);
    Map<String, Object> rounded = new ObjectMapper().readValue(
        "{\"value\":9007199254740993.0,\"units\":\"\"}",
        new TypeReference<Map<String, Object>>() { });
    allocations.put("example.com/rounded", rounded);
    Map<String, Object> valid = new HashMap<>();
    valid.put("value", 2);
    valid.put("units", "");
    allocations.put("yarn.io/gpu", valid);
    info.put(ContainerMetricsConstants.ALLOCATED_RESOURCES_INFO, allocations);

    Resource restored = TimelineUtils.getContainerResource(info);
    assertEquals(1024, restored.getMemorySize());
    assertEquals(2, restored.getVirtualCores());
    assertEquals(0, restored.getResourceValue("example.com/fpga"));
    assertEquals(0, restored.getResourceValue("example.com/bandwidth"));
    assertEquals(0, restored.getResourceValue("example.com/overflow"));
    assertEquals(0, restored.getResourceValue("example.com/invalid"));
    assertEquals(0, restored.getResourceValue("example.com/oversized"));
    assertEquals(0, restored.getResourceValue("example.com/fractional"));
    assertEquals(0, restored.getResourceValue("example.com/rounded"));
    assertEquals(2, restored.getResourceValue("yarn.io/gpu"));

    info.put(ContainerMetricsConstants.ALLOCATED_RESOURCES_INFO, "invalid");
    assertEquals(1024, TimelineUtils.getContainerResource(info).getMemorySize());
  }

  @Test
  void testNoCustomResourceInfo() {
    ResourceUtils.resetResourceTypes(new YarnConfiguration());
    assertThat(TimelineUtils.getCustomResourceInfo(Resource.newInstance(1024, 2)))
        .isEmpty();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RESOURCE_TYPES, "yarn.io/gpu");
    ResourceUtils.resetResourceTypes(conf);
    Resource allocated = Resource.newInstance(1024, 2);
    assertThat(TimelineUtils.getCustomResourceInfo(allocated)).isEmpty();
    allocated.setResourceValue("yarn.io/gpu", 1);
    assertThat(TimelineUtils.getCustomResourceInfo(allocated))
        .containsKey("yarn.io/gpu");
  }

  @Test
  void testMapCastToHashMap() {

    // Test null map be casted to null
    Map<String, String> nullMap = null;
    assertNull(TimelineServiceHelper.mapCastToHashMap(nullMap));

    // Test empty hashmap be casted to a empty hashmap
    Map<String, String> emptyHashMap = new HashMap<String, String>();
    assertEquals(
        TimelineServiceHelper.mapCastToHashMap(emptyHashMap).size(), 0);

    // Test empty non-hashmap be casted to a empty hashmap
    Map<String, String> emptyTreeMap = new TreeMap<String, String>();
    assertEquals(
        TimelineServiceHelper.mapCastToHashMap(emptyTreeMap).size(), 0);

    // Test non-empty hashmap be casted to hashmap correctly
    Map<String, String> firstHashMap = new HashMap<String, String>();
    String key = "KEY";
    String value = "VALUE";
    firstHashMap.put(key, value);
    assertEquals(
        TimelineServiceHelper.mapCastToHashMap(firstHashMap), firstHashMap);

    // Test non-empty non-hashmap is casted correctly.
    Map<String, String> firstTreeMap = new TreeMap<String, String>();
    firstTreeMap.put(key, value);
    HashMap<String, String> alternateHashMap =
        TimelineServiceHelper.mapCastToHashMap(firstTreeMap);
    assertEquals(firstTreeMap.size(), alternateHashMap.size());
    assertThat(alternateHashMap.get(key)).isEqualTo(value);

    // Test complicated hashmap be casted correctly
    Map<String, Set<String>> complicatedHashMap =
        new HashMap<String, Set<String>>();
    Set<String> hashSet = new HashSet<String>();
    hashSet.add(value);
    complicatedHashMap.put(key, hashSet);
    assertEquals(
        TimelineServiceHelper.mapCastToHashMap(complicatedHashMap),
        complicatedHashMap);

    // Test complicated non-hashmap get casted correctly
    Map<String, Set<String>> complicatedTreeMap =
        new TreeMap<String, Set<String>>();
    complicatedTreeMap.put(key, hashSet);
    assertEquals(
        TimelineServiceHelper.mapCastToHashMap(complicatedTreeMap).get(key),
        hashSet);
  }

}

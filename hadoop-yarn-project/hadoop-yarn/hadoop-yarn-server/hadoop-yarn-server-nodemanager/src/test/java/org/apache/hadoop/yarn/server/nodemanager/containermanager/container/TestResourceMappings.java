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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceAllocation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDevice;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class TestResourceMappings {

  private static final ResourceMappings.AssignedResources testResources =
      new ResourceMappings.AssignedResources();

  /**
   * A NUMA assigned-resources record as written by released NodeManagers,
   * captured by running each release's own
   * {@code AssignedResources.toBytes()} with its hadoop-shaded-guava. Every
   * release from 3.3.1 through 3.5.0 (hadoop-shaded-guava 1.1.1 through
   * 1.5.0) writes these exact bytes. The ImmutableMaps inside
   * NumaResourceAllocation travel as the shaded ImmutableBiMap (single-entry
   * map) and ImmutableMap serialization proxies, which is why those two names
   * are on the fromBytes allowlist.
   *
   * This record holds {@code new NumaResourceAllocation("0", 1024L, "0", 4)},
   * the single-node path of NumaResourceAllocator.allocate.
   */
  private static final String NUMA_SINGLE_NODE_RECORD =
      "rO0ABXNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAAABdwQAAAAB"
      + "c3IAZm9yZy5hcGFjaGUuaGFkb29wLnlhcm4uc2VydmVyLm5vZGVtYW5hZ2VyLmNvbnRhaW5lcm1h"
      + "bmFnZXIubGludXgucmVzb3VyY2VzLm51bWEuTnVtYVJlc291cmNlQWxsb2NhdGlvblf7NZFB65wz"
      + "AgACTAAKbm9kZVZzQ3B1c3QARUxvcmcvYXBhY2hlL2hhZG9vcC90aGlyZHBhcnR5L2NvbS9nb29n"
      + "bGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTWFwO0wADG5vZGVWc01lbW9yeXEAfgADeHBzcgBU"
      + "b3JnLmFwYWNoZS5oYWRvb3AudGhpcmRwYXJ0eS5jb20uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0Lklt"
      + "bXV0YWJsZUJpTWFwJFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAB4cgBSb3JnLmFwYWNoZS5oYWRv"
      + "b3AudGhpcmRwYXJ0eS5jb20uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0LkltbXV0YWJsZU1hcCRTZXJp"
      + "YWxpemVkRm9ybQAAAAAAAAAAAgACTAAEa2V5c3QAEkxqYXZhL2xhbmcvT2JqZWN0O0wABnZhbHVl"
      + "c3EAfgAHeHB1cgATW0xqYXZhLmxhbmcuT2JqZWN0O5DOWJ8QcylsAgAAeHAAAAABdAABMHVxAH4A"
      + "CQAAAAFzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFu"
      + "Zy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAABHNxAH4ABXVxAH4ACQAAAAFxAH4AC3VxAH4ACQAAAAFz"
      + "cgAOamF2YS5sYW5nLkxvbmc7i+SQzI8j3wIAAUoABXZhbHVleHEAfgAOAAAAAAAABAB4";
  /**
   * Multi-node counterpart of {@link #NUMA_SINGLE_NODE_RECORD}, same releases:
   * {@code new NumaResourceAllocation({"0": 2048, "1": 1024}, {"0": 4, "1": 2})}.
   */
  private static final String NUMA_MULTI_NODE_RECORD =
      "rO0ABXNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAAABdwQAAAAB"
      + "c3IAZm9yZy5hcGFjaGUuaGFkb29wLnlhcm4uc2VydmVyLm5vZGVtYW5hZ2VyLmNvbnRhaW5lcm1h"
      + "bmFnZXIubGludXgucmVzb3VyY2VzLm51bWEuTnVtYVJlc291cmNlQWxsb2NhdGlvblf7NZFB65wz"
      + "AgACTAAKbm9kZVZzQ3B1c3QARUxvcmcvYXBhY2hlL2hhZG9vcC90aGlyZHBhcnR5L2NvbS9nb29n"
      + "bGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTWFwO0wADG5vZGVWc01lbW9yeXEAfgADeHBzcgBS"
      + "b3JnLmFwYWNoZS5oYWRvb3AudGhpcmRwYXJ0eS5jb20uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0Lklt"
      + "bXV0YWJsZU1hcCRTZXJpYWxpemVkRm9ybQAAAAAAAAAAAgACTAAEa2V5c3QAEkxqYXZhL2xhbmcv"
      + "T2JqZWN0O0wABnZhbHVlc3EAfgAGeHB1cgATW0xqYXZhLmxhbmcuT2JqZWN0O5DOWJ8QcylsAgAA"
      + "eHAAAAACdAABMHQAATF1cQB+AAgAAAACc3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJ"
      + "AAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAARzcQB+AA0AAAACc3EA"
      + "fgAFdXEAfgAIAAAAAnEAfgAKcQB+AAt1cQB+AAgAAAACc3IADmphdmEubGFuZy5Mb25nO4vkkMyP"
      + "I98CAAFKAAV2YWx1ZXhxAH4ADgAAAAAAAAgAc3EAfgAUAAAAAAAABAB4";

  @BeforeAll
  public static void setup() {
    testResources.updateAssignedResources(ImmutableList.of(
        Device.Builder.newInstance()
            .setId(0)
            .setDevPath("/dev/hdwA0")
            .setMajorNumber(256)
            .setMinorNumber(0)
            .setBusID("0000:80:00.0")
            .setHealthy(true)
            .build(),
        Device.Builder.newInstance()
            .setId(1)
            .setDevPath("/dev/hdwA1")
            .setMajorNumber(256)
            .setMinorNumber(0)
            .setBusID("0000:80:00.1")
            .setHealthy(true)
            .build()
    ));
  }

  @Test
  public void testSerializeAssignedResourcesWithSerializationUtils() {
    try {
      byte[] serializedString = testResources.toBytes();

      ResourceMappings.AssignedResources deserialized =
          ResourceMappings.AssignedResources.fromBytes(serializedString);

      assertEquals(testResources.getAssignedResources(),
          deserialized.getAssignedResources());

    } catch (IOException e) {
      e.printStackTrace();
      fail(String.format("Serialization of test AssignedResources " +
          "failed with %s", e.getMessage()));
    }
  }

  @Test
  public void testAssignedResourcesCanDeserializePreviouslySerializedValues() {
    try {
      byte[] serializedString = toBytes(testResources.getAssignedResources());

      ResourceMappings.AssignedResources deserialized =
          ResourceMappings.AssignedResources.fromBytes(serializedString);

      assertEquals(testResources.getAssignedResources(),
          deserialized.getAssignedResources());

    } catch (IOException e) {
      e.printStackTrace();
      fail(String.format("Deserialization of test AssignedResources " +
          "failed with %s", e.getMessage()));
    }
  }

  @Test
  public void testRoundTripCoversResourcePluginTypes() throws IOException {
    // The elements a NodeManager actually stores for gpu / fpga / numa
    // resources must survive the allowlist, otherwise recovery would break.
    ResourceMappings.AssignedResources pluginResources =
        new ResourceMappings.AssignedResources();
    pluginResources.updateAssignedResources(ImmutableList.of(
        new GpuDevice(2, 3),
        new FpgaDevice("IntelOpenCL", 247, 0, "aclv0"),
        new NumaResourceAllocation("0", 1024L, "0", 4),
        "cpu-0"));

    ResourceMappings.AssignedResources deserialized =
        ResourceMappings.AssignedResources.fromBytes(pluginResources.toBytes());

    assertEquals(pluginResources.getAssignedResources(),
        deserialized.getAssignedResources());
  }

  @Test
  public void testFromBytesReadsNumaRecordsFromPriorReleases()
      throws IOException {
    ResourceMappings.AssignedResources singleNode =
        ResourceMappings.AssignedResources.fromBytes(
            Base64.getDecoder().decode(NUMA_SINGLE_NODE_RECORD));
    assertEquals(
        Collections.singletonList(new NumaResourceAllocation("0", 1024L, "0", 4)),
        singleNode.getAssignedResources());

    ResourceMappings.AssignedResources multiNode =
        ResourceMappings.AssignedResources.fromBytes(
            Base64.getDecoder().decode(NUMA_MULTI_NODE_RECORD));
    assertEquals(
        Collections.singletonList(new NumaResourceAllocation(
            ImmutableMap.of("0", 2048L, "1", 1024L),
            ImmutableMap.of("0", 4, "1", 2))),
        multiNode.getAssignedResources());
  }

  @Test
  public void testFromBytesRejectsUnexpectedType() throws IOException {
    // A tampered record whose top-level list is fine but which carries an
    // element of a type the resource plugins never store. This stands in for a
    // serialization gadget (e.g. a commons-beanutils BeanComparator): the
    // allowlist rejects it by class name during readObject, before the object
    // is instantiated and any of its logic runs.
    List<Serializable> tampered = new ArrayList<>();
    tampered.add(new File("/etc/passwd"));
    byte[] payload = toBytes(tampered);
    assertThrows(IOException.class,
        () -> ResourceMappings.AssignedResources.fromBytes(payload));
  }

  @Test
  public void testFromBytesRejectsGuavaTypesOutsideTheAllowlist()
      throws IOException {
    // Only the two ImmutableMap serialization proxies are accepted, not the
    // shaded-guava collect package as a whole: an ImmutableList is rejected
    // both as the record itself and as an element of an allowed ArrayList.
    byte[] topLevel = toBytes(ImmutableList.<Serializable>of("cpu-0"));
    assertThrows(IOException.class,
        () -> ResourceMappings.AssignedResources.fromBytes(topLevel));

    List<Serializable> wrapped = new ArrayList<>();
    wrapped.add(ImmutableList.of("cpu-0"));
    byte[] element = toBytes(wrapped);
    assertThrows(IOException.class,
        () -> ResourceMappings.AssignedResources.fromBytes(element));
  }

  /**
   * This was the legacy way to serialize resources. This is here for
   * backward compatibility to ensure that after YARN-9128 we can still
   * deserialize previously serialized resources.
   *
   * @param resources the list of resources
   * @return byte array representation of the resource
   * @throws IOException
   */
  private byte[] toBytes(List<Serializable> resources) throws IOException {
    byte[] bytes;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(resources);
      bytes = bos.toByteArray();
    }
    return bytes;
  }
}
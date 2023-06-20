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

package org.apache.hadoop.yarn.api;

import java.io.File;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class to handle various proto related tests for resources.
 */
public class TestResourcePBImpl {

  @BeforeEach
  public void setup() throws Exception {
    ResourceUtils.resetResourceTypes();

    String resourceTypesFile = "resource-types-5.xml";
    Configuration conf = new YarnConfiguration();
    TestResourceUtils.setupResourceTypes(conf, resourceTypesFile);
  }

  @AfterEach
  public void teardown() {
    Configuration conf = new YarnConfiguration();
    File source = new File(
        conf.getClassLoader().getResource("resource-types-5.xml").getFile());
    File dest = new File(source.getParent(), "resource-types.xml");
    if (dest.exists()) {
      dest.delete();
    }
  }

  @Test
  void testEmptyResourcePBInit() throws Exception {
    Resource res = new ResourcePBImpl();
    // Assert to check it sets resource value and unit to default.
    assertEquals(0, res.getMemorySize());
    assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        res.getResourceInformation(ResourceInformation.MEMORY_MB.getName())
            .getUnits());
    assertEquals(ResourceInformation.VCORES.getUnits(),
        res.getResourceInformation(ResourceInformation.VCORES.getName())
            .getUnits());
  }

  @Test
  void testResourcePBInitFromOldPB() throws Exception {
    YarnProtos.ResourceProto proto =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).setVirtualCores(3)
            .build();
    // Assert to check it sets resource value and unit to default.
    Resource res = new ResourcePBImpl(proto);
    assertEquals(1024, res.getMemorySize());
    assertEquals(3, res.getVirtualCores());
    assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        res.getResourceInformation(ResourceInformation.MEMORY_MB.getName())
            .getUnits());
    assertEquals(ResourceInformation.VCORES.getUnits(),
        res.getResourceInformation(ResourceInformation.VCORES.getName())
            .getUnits());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testGetMemory() {
    Resource res = new ResourcePBImpl();
    long memorySize = Integer.MAX_VALUE + 1L;
    res.setMemorySize(memorySize);

    assertEquals(memorySize, res.getMemorySize(), "No need to cast if both are long");
    assertEquals(Integer.MAX_VALUE, res.getMemory(),
        "Cast to Integer.MAX_VALUE if the long is greater than " + "Integer.MAX_VALUE");
  }

  @Test
  void testGetVirtualCores() {
    Resource res = new ResourcePBImpl();
    long vcores = Integer.MAX_VALUE + 1L;
    res.getResourceInformation("vcores").setValue(vcores);

    assertEquals(vcores,
        res.getResourceInformation("vcores").getValue(),
        "No need to cast if both are long");
    assertEquals(Integer.MAX_VALUE, res.getVirtualCores(),
        "Cast to Integer.MAX_VALUE if the long is greater than " + "Integer.MAX_VALUE");
  }

  @Test
  void testResourcePBWithExtraResources() throws Exception {

    //Resource 'resource1' has been passed as 4T
    //4T should be converted to 4000G
    YarnProtos.ResourceInformationProto riProto =
        YarnProtos.ResourceInformationProto.newBuilder().setType(
            YarnProtos.ResourceTypeInfoProto.newBuilder().
                setName("resource1").setType(
                YarnProtos.ResourceTypesProto.COUNTABLE).getType()).
            setValue(4).setUnits("T").setKey("resource1").build();

    YarnProtos.ResourceProto proto =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).
            setVirtualCores(3).addResourceValueMap(riProto).build();
    Resource res = new ResourcePBImpl(proto);

    assertEquals(4000,
        res.getResourceInformation("resource1").getValue());
    assertEquals("G",
        res.getResourceInformation("resource1").getUnits());

    //Resource 'resource2' has been passed as 4M
    //4M should be converted to 4000000000m
    YarnProtos.ResourceInformationProto riProto1 =
        YarnProtos.ResourceInformationProto.newBuilder().setType(
            YarnProtos.ResourceTypeInfoProto.newBuilder().
                setName("resource2").setType(
                YarnProtos.ResourceTypesProto.COUNTABLE).getType()).
            setValue(4).setUnits("M").setKey("resource2").build();

    YarnProtos.ResourceProto proto1 =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).
            setVirtualCores(3).addResourceValueMap(riProto1).build();
    Resource res1 = new ResourcePBImpl(proto1);

    assertEquals(4000000000L,
        res1.getResourceInformation("resource2").getValue());
    assertEquals("m",
        res1.getResourceInformation("resource2").getUnits());

    //Resource 'resource1' has been passed as 3M
    //3M should be converted to 0G
    YarnProtos.ResourceInformationProto riProto2 =
        YarnProtos.ResourceInformationProto.newBuilder().setType(
            YarnProtos.ResourceTypeInfoProto.newBuilder().
                setName("resource1").setType(
                YarnProtos.ResourceTypesProto.COUNTABLE).getType()).
            setValue(3).setUnits("M").setKey("resource1").build();

    YarnProtos.ResourceProto proto2 =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).
            setVirtualCores(3).addResourceValueMap(riProto2).build();
    Resource res2 = new ResourcePBImpl(proto2);

    assertEquals(0,
        res2.getResourceInformation("resource1").getValue());
    assertEquals("G",
        res2.getResourceInformation("resource1").getUnits());
  }

  @Test
  void testResourceTags() {
    YarnProtos.ResourceInformationProto riProto =
        YarnProtos.ResourceInformationProto.newBuilder()
            .setType(
                YarnProtos.ResourceTypeInfoProto.newBuilder()
                    .setName("yarn.io/test-volume")
                    .setType(YarnProtos.ResourceTypesProto.COUNTABLE).getType())
            .setValue(10)
            .setUnits("G")
            .setKey("yarn.io/test-volume")
            .addTags("tag_A")
            .addTags("tag_B")
            .addTags("tag_C")
            .build();
    YarnProtos.ResourceProto proto =
        YarnProtos.ResourceProto.newBuilder()
            .setMemory(1024)
            .setVirtualCores(3)
            .addResourceValueMap(riProto)
            .build();
    Resource res = new ResourcePBImpl(proto);

    assertNotNull(res.getResourceInformation("yarn.io/test-volume"));
    assertEquals(10,
        res.getResourceInformation("yarn.io/test-volume")
            .getValue());
    assertEquals("G",
        res.getResourceInformation("yarn.io/test-volume")
            .getUnits());
    assertEquals(3,
        res.getResourceInformation("yarn.io/test-volume")
            .getTags().size());
    assertFalse(res.getResourceInformation("yarn.io/test-volume")
        .getTags().isEmpty());
    assertTrue(res.getResourceInformation("yarn.io/test-volume")
        .getAttributes().isEmpty());

    boolean protoConvertExpected = false;
    YarnProtos.ResourceProto protoFormat = ProtoUtils.convertToProtoFormat(res);
    for (YarnProtos.ResourceInformationProto pf :
        protoFormat.getResourceValueMapList()) {
      if (pf.getKey().equals("yarn.io/test-volume")) {
        protoConvertExpected = pf.getAttributesCount() == 0
            && pf.getTagsCount() == 3;
      }
    }
    assertTrue(protoConvertExpected,
        "Expecting resource's protobuf message"
            + " contains 0 attributes and 3 tags");
  }

  @Test
  void testResourceAttributes() {
    YarnProtos.ResourceInformationProto riProto =
        YarnProtos.ResourceInformationProto.newBuilder()
            .setType(
                YarnProtos.ResourceTypeInfoProto.newBuilder()
                    .setName("yarn.io/test-volume")
                    .setType(YarnProtos.ResourceTypesProto.COUNTABLE).getType())
            .setValue(10)
            .setUnits("G")
            .setKey("yarn.io/test-volume")
            .addAttributes(
                YarnProtos.StringStringMapProto
                    .newBuilder()
                    .setKey("driver").setValue("test-driver")
                    .build())
            .addAttributes(
                YarnProtos.StringStringMapProto
                    .newBuilder()
                    .setKey("mount").setValue("/mnt/data")
                    .build())
            .build();
    YarnProtos.ResourceProto proto =
        YarnProtos.ResourceProto.newBuilder()
            .setMemory(1024)
            .setVirtualCores(3)
            .addResourceValueMap(riProto)
            .build();
    Resource res = new ResourcePBImpl(proto);

    assertNotNull(res.getResourceInformation("yarn.io/test-volume"));
    assertEquals(10,
        res.getResourceInformation("yarn.io/test-volume")
            .getValue());
    assertEquals("G",
        res.getResourceInformation("yarn.io/test-volume")
            .getUnits());
    assertEquals(2,
        res.getResourceInformation("yarn.io/test-volume")
            .getAttributes().size());
    assertTrue(res.getResourceInformation("yarn.io/test-volume")
        .getTags().isEmpty());
    assertFalse(res.getResourceInformation("yarn.io/test-volume")
        .getAttributes().isEmpty());

    boolean protoConvertExpected = false;
    YarnProtos.ResourceProto protoFormat = ProtoUtils.convertToProtoFormat(res);
    for (YarnProtos.ResourceInformationProto pf :
        protoFormat.getResourceValueMapList()) {
      if (pf.getKey().equals("yarn.io/test-volume")) {
        protoConvertExpected = pf.getAttributesCount() == 2
            && pf.getTagsCount() == 0;
      }
    }
    assertTrue(protoConvertExpected,
        "Expecting resource's protobuf message"
            + " contains 2 attributes and 0 tags");
  }

  @Test
  void testParsingResourceTags() {
    ResourceInformation info =
        ResourceUtils.getResourceTypes().get("resource3");
    assertTrue(info.getAttributes().isEmpty());
    assertFalse(info.getTags().isEmpty());
    assertThat(info.getTags()).hasSize(2);
    info.getTags().remove("resource3_tag_1");
    info.getTags().remove("resource3_tag_2");
    assertTrue(info.getTags().isEmpty());
  }
}

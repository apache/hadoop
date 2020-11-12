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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

public class TestResourceMappings {

  private static final ResourceMappings.AssignedResources testResources =
      new ResourceMappings.AssignedResources();

  @BeforeClass
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

      Assert.assertEquals(testResources.getAssignedResources(),
          deserialized.getAssignedResources());

    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(String.format("Serialization of test AssignedResources " +
          "failed with %s", e.getMessage()));
    }
  }

  @Test
  public void testAssignedResourcesCanDeserializePreviouslySerializedValues() {
    try {
      byte[] serializedString = toBytes(testResources.getAssignedResources());

      ResourceMappings.AssignedResources deserialized =
          ResourceMappings.AssignedResources.fromBytes(serializedString);

      Assert.assertEquals(testResources.getAssignedResources(),
          deserialized.getAssignedResources());

    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(String.format("Deserialization of test AssignedResources " +
          "failed with %s", e.getMessage()));
    }
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
    ObjectOutputStream oos = null;
    byte[] bytes;
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(resources);
      bytes = bos.toByteArray();
    } finally {
      IOUtils.closeQuietly(oos);
    }
    return bytes;
  }
}
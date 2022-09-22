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
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.JsonSerialization;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestJsonUtilClient {
  @Test
  public void testToStringArray() {
    List<String> strList = new ArrayList<String>(Arrays.asList("aaa", "bbb", "ccc"));

    String[] strArr = JsonUtilClient.toStringArray(strList);
    assertEquals("Expected 3 items in the array", 3, strArr.length);
    assertEquals("aaa", strArr[0]);
    assertEquals("bbb", strArr[1]);
    assertEquals("ccc", strArr[2]);
  }

  @Test
  public void testToBlockLocationArray() throws Exception {
    BlockLocation blockLocation = new BlockLocation(
        new String[] {"127.0.0.1:62870"},
        new String[] {"127.0.0.1"},
        null,
        new String[] {"/default-rack/127.0.0.1:62870"},
        null,
        new StorageType[] {StorageType.DISK},
        0, 1, false);

    Map<String, Object> blockLocationsMap =
        JsonUtil.toJsonMap(new BlockLocation[] {blockLocation});
    String json = JsonUtil.toJsonString("BlockLocations", blockLocationsMap);
    assertNotNull(json);
    Map<?, ?> jsonMap = JsonSerialization.mapReader().readValue(json);

    BlockLocation[] deserializedBlockLocations =
        JsonUtilClient.toBlockLocationArray(jsonMap);
    assertEquals(1, deserializedBlockLocations.length);
    assertEquals(blockLocation.toString(),
        deserializedBlockLocations[0].toString());
  }
}

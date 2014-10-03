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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.TestContainerId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.URL;
import org.junit.Test;

public class TestConverterUtils {
  
  @Test
  public void testConvertUrlWithNoPort() throws URISyntaxException {
    Path expectedPath = new Path("hdfs://foo.com");
    URL url = ConverterUtils.getYarnUrlFromPath(expectedPath);
    Path actualPath = ConverterUtils.getPathFromYarnURL(url);
    assertEquals(expectedPath, actualPath);
  }

  @Test
  public void testConvertUrlWithUserinfo() throws URISyntaxException {
    Path expectedPath = new Path("foo://username:password@example.com:8042");
    URL url = ConverterUtils.getYarnUrlFromPath(expectedPath);
    Path actualPath = ConverterUtils.getPathFromYarnURL(url);
    assertEquals(expectedPath, actualPath);
  }
  
  @Test
  public void testContainerId() throws URISyntaxException {
    ContainerId id = TestContainerId.newContainerId(0, 0, 0, 0);
    String cid = ConverterUtils.toString(id);
    assertEquals("container_0_0000_00_000000", cid);
    ContainerId gen = ConverterUtils.toContainerId(cid);
    assertEquals(gen, id);
  }

  @Test
  public void testContainerIdWithEpoch() throws URISyntaxException {
    ContainerId id = TestContainerId.newContainerId(0, 0, 0, 25645811);
    String cid = ConverterUtils.toString(id);
    assertEquals("container_0_0000_00_25645811", cid);
    ContainerId gen = ConverterUtils.toContainerId(cid);
    assertEquals(gen.toString(), id.toString());

    long ts = System.currentTimeMillis();
    ContainerId id2 =
        TestContainerId.newContainerId(36473, 4365472, ts, 4298334883325L);
    String cid2 = ConverterUtils.toString(id2);
    assertEquals(
        "container_e03_" + ts + "_36473_4365472_999799999997", cid2);
    ContainerId gen2 = ConverterUtils.toContainerId(cid2);
    assertEquals(gen2.toString(), id2.toString());

    ContainerId id3 =
        TestContainerId.newContainerId(36473, 4365472, ts, 844424930131965L);
    String cid3 = ConverterUtils.toString(id3);
    assertEquals(
        "container_e767_" + ts + "_36473_4365472_1099511627773", cid3);
    ContainerId gen3 = ConverterUtils.toContainerId(cid3);
    assertEquals(gen3.toString(), id3.toString());
  }

  @Test
  public void testContainerIdNull() throws URISyntaxException {
    assertNull(ConverterUtils.toString((ContainerId)null));
  }  
}

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.TestContainerId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.Test;

public class TestConverterUtils {
  
  @Test
  public void testConvertUrlWithNoPort() throws URISyntaxException {
    Path expectedPath = new Path("hdfs://foo.com");
    URL url = URL.fromPath(expectedPath);
    Path actualPath = url.toPath();
    assertEquals(expectedPath, actualPath);
  }

  @Test
  public void testConvertUrlWithUserinfo() throws URISyntaxException {
    Path expectedPath = new Path("foo://username:password@example.com:8042");
    URL url = URL.fromPath(expectedPath);
    Path actualPath = url.toPath();
    assertEquals(expectedPath, actualPath);
  }
  
  @Test
  public void testContainerId() throws URISyntaxException {
    ContainerId id = TestContainerId.newContainerId(0, 0, 0, 0);
    String cid = id.toString();
    assertEquals("container_0_0000_00_000000", cid);
    ContainerId gen = ContainerId.fromString(cid);
    assertEquals(gen, id);
  }

  @Test
  public void testContainerIdWithEpoch() throws URISyntaxException {
    ContainerId id = TestContainerId.newContainerId(0, 0, 0, 25645811);
    String cid = id.toString();
    assertEquals("container_0_0000_00_25645811", cid);
    ContainerId gen = ContainerId.fromString(cid);
    assertEquals(gen.toString(), id.toString());

    long ts = System.currentTimeMillis();
    ContainerId id2 =
        TestContainerId.newContainerId(36473, 4365472, ts, 4298334883325L);
    String cid2 = id2.toString();
    assertEquals(
        "container_e03_" + ts + "_36473_4365472_999799999997", cid2);
    ContainerId gen2 = ContainerId.fromString(cid2);
    assertEquals(gen2.toString(), id2.toString());

    ContainerId id3 =
        TestContainerId.newContainerId(36473, 4365472, ts, 844424930131965L);
    String cid3 = id3.toString();
    assertEquals(
        "container_e767_" + ts + "_36473_4365472_1099511627773", cid3);
    ContainerId gen3 = ContainerId.fromString(cid3);
    assertEquals(gen3.toString(), id3.toString());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testContainerIdNull() throws URISyntaxException {
    assertNull(ConverterUtils.toString((ContainerId)null));
  }  
  
  @Test
  public void testNodeIdWithDefaultPort() throws URISyntaxException {
    NodeId nid;
    
    nid = ConverterUtils.toNodeIdWithDefaultPort("node:10");
    assertThat(nid.getPort()).isEqualTo(10);
    assertThat(nid.getHost()).isEqualTo("node");
    
    nid = ConverterUtils.toNodeIdWithDefaultPort("node");
    assertThat(nid.getPort()).isEqualTo(0);
    assertThat(nid.getHost()).isEqualTo("node");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("deprecation")
  public void testInvalidContainerId() {
    ContainerId.fromString("container_e20_1423221031460_0003_01");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("deprecation")
  public void testInvalidAppattemptId() {
    ConverterUtils.toApplicationAttemptId("appattempt_1423221031460");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("deprecation")
  public void testApplicationId() {
    ConverterUtils.toApplicationId("application_1423221031460");
  }
}

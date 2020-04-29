/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerCluster;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerDataNode;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolume;
import org.apache.hadoop.hdfs.server.diskbalancer.datamodel.DiskBalancerVolumeSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Tests DiskBalancer Data models.
 */
public class TestDataModels {
  @Test
  public void testCreateRandomVolume() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerVolume vol = util.createRandomVolume(StorageType.DISK);
    Assert.assertNotNull(vol.getUuid());
    Assert.assertNotNull(vol.getPath());
    Assert.assertNotNull(vol.getStorageType());
    Assert.assertFalse(vol.isFailed());
    Assert.assertFalse(vol.isTransient());
    Assert.assertTrue(vol.getCapacity() > 0);
    Assert.assertTrue((vol.getCapacity() - vol.getReserved()) > 0);
    Assert.assertTrue((vol.getReserved() + vol.getUsed()) < vol.getCapacity());
  }

  @Test
  public void testCreateRandomVolumeSet() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerVolumeSet vSet =
        util.createRandomVolumeSet(StorageType.SSD, 10);
    Assert.assertEquals(10, vSet.getVolumeCount());
    Assert.assertEquals(StorageType.SSD.toString(),
        vSet.getVolumes().get(0).getStorageType());

  }

  @Test
  public void testCreateRandomDataNode() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerDataNode node = util.createRandomDataNode(
        new StorageType[]{StorageType.DISK, StorageType.RAM_DISK}, 10);
    Assert.assertNotNull(node.getNodeDataDensity());
  }

  @Test
  public void testDiskQueues() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerDataNode node = util.createRandomDataNode(
        new StorageType[]{StorageType.DISK, StorageType.RAM_DISK}, 3);
    TreeSet<DiskBalancerVolume> sortedQueue =
        node.getVolumeSets().get(StorageType.DISK.toString()).getSortedQueue();

    List<DiskBalancerVolume> reverseList = new LinkedList<>();
    List<DiskBalancerVolume> highList = new LinkedList<>();
    int queueSize = sortedQueue.size();
    for (int x = 0; x < queueSize; x++) {
      reverseList.add(sortedQueue.first());
      highList.add(sortedQueue.first());
    }
    Collections.reverse(reverseList);

    for (int x = 0; x < queueSize; x++) {

      Assert.assertEquals(reverseList.get(x).getCapacity(),
          highList.get(x).getCapacity());
      Assert.assertEquals(reverseList.get(x).getReserved(),
          highList.get(x).getReserved());
      Assert.assertEquals(reverseList.get(x).getUsed(),
          highList.get(x).getUsed());
    }
  }

  @Test
  public void testNoBalancingNeededEvenDataSpread() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    // create two disks which have exactly same data and isBalancing should
    // say we don't need to balance.
    DiskBalancerVolume v1 = util.createRandomVolume(StorageType.SSD);
    v1.setCapacity(DiskBalancerTestUtil.TB);
    v1.setReserved(100 * DiskBalancerTestUtil.GB);
    v1.setUsed(500 * DiskBalancerTestUtil.GB);

    DiskBalancerVolume v2 = util.createRandomVolume(StorageType.SSD);
    v2.setCapacity(DiskBalancerTestUtil.TB);
    v2.setReserved(100 * DiskBalancerTestUtil.GB);
    v2.setUsed(500 * DiskBalancerTestUtil.GB);

    node.addVolume(v1);
    node.addVolume(v2);

    for (DiskBalancerVolumeSet vsets : node.getVolumeSets().values()) {
      Assert.assertFalse(vsets.isBalancingNeeded(10.0f));
    }
  }

  @Test
  public void testNoBalancingNeededTransientDisks() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    // create two disks which have different data sizes, but
    // transient. isBalancing should say no balancing needed.
    DiskBalancerVolume v1 = util.createRandomVolume(StorageType.RAM_DISK);
    v1.setCapacity(DiskBalancerTestUtil.TB);
    v1.setReserved(100 * DiskBalancerTestUtil.GB);
    v1.setUsed(1 * DiskBalancerTestUtil.GB);

    DiskBalancerVolume v2 = util.createRandomVolume(StorageType.RAM_DISK);
    v2.setCapacity(DiskBalancerTestUtil.TB);
    v2.setReserved(100 * DiskBalancerTestUtil.GB);
    v2.setUsed(500 * DiskBalancerTestUtil.GB);

    node.addVolume(v1);
    node.addVolume(v2);

    for (DiskBalancerVolumeSet vsets : node.getVolumeSets().values()) {
      Assert.assertFalse(vsets.isBalancingNeeded(10.0f));
    }
  }

  @Test
  public void testNoBalancingNeededFailedDisks() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    // create two disks which have which are normal disks, but fail
    // one of them. VolumeSet should say no balancing needed.
    DiskBalancerVolume v1 = util.createRandomVolume(StorageType.SSD);
    v1.setCapacity(DiskBalancerTestUtil.TB);
    v1.setReserved(100 * DiskBalancerTestUtil.GB);
    v1.setUsed(1 * DiskBalancerTestUtil.GB);
    v1.setFailed(true);

    DiskBalancerVolume v2 = util.createRandomVolume(StorageType.SSD);
    v2.setCapacity(DiskBalancerTestUtil.TB);
    v2.setReserved(100 * DiskBalancerTestUtil.GB);
    v2.setUsed(500 * DiskBalancerTestUtil.GB);

    node.addVolume(v1);
    node.addVolume(v2);

    for (DiskBalancerVolumeSet vsets : node.getVolumeSets().values()) {
      Assert.assertFalse(vsets.isBalancingNeeded(10.0f));
    }
  }

  @Test
  public void testNeedBalancingUnevenDataSpread() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerDataNode node =
        new DiskBalancerDataNode(UUID.randomUUID().toString());

    DiskBalancerVolume v1 = util.createRandomVolume(StorageType.SSD);
    v1.setCapacity(DiskBalancerTestUtil.TB);
    v1.setReserved(100 * DiskBalancerTestUtil.GB);
    v1.setUsed(0);

    DiskBalancerVolume v2 = util.createRandomVolume(StorageType.SSD);
    v2.setCapacity(DiskBalancerTestUtil.TB);
    v2.setReserved(100 * DiskBalancerTestUtil.GB);
    v2.setUsed(500 * DiskBalancerTestUtil.GB);

    node.addVolume(v1);
    node.addVolume(v2);

    for (DiskBalancerVolumeSet vsets : node.getVolumeSets().values()) {
      Assert.assertTrue(vsets.isBalancingNeeded(10.0f));
    }
  }

  @Test
  public void testVolumeSerialize() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();
    DiskBalancerVolume volume = util.createRandomVolume(StorageType.DISK);
    String originalString = volume.toJson();
    DiskBalancerVolume parsedVolume =
        DiskBalancerVolume.parseJson(originalString);
    String parsedString = parsedVolume.toJson();
    Assert.assertEquals(originalString, parsedString);
  }

  @Test
  public void testClusterSerialize() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();

    // Create a Cluster with 3 datanodes, 3 disk types and 3 disks in each type
    // that is 9 disks in each machine.
    DiskBalancerCluster cluster = util.createRandCluster(3, new StorageType[]{
        StorageType.DISK, StorageType.RAM_DISK, StorageType.SSD}, 3);

    DiskBalancerCluster newCluster =
        DiskBalancerCluster.parseJson(cluster.toJson());
    Assert.assertEquals(cluster.getNodes(), newCluster.getNodes());
    Assert
        .assertEquals(cluster.getNodes().size(), newCluster.getNodes().size());
  }

  @Test
  public void testUsageLimitedToCapacity() throws Exception {
    DiskBalancerTestUtil util = new DiskBalancerTestUtil();

    // If usage is greater than capacity, then it should be set to capacity
    DiskBalancerVolume v1 = util.createRandomVolume(StorageType.DISK);
    v1.setCapacity(DiskBalancerTestUtil.GB);
    v1.setUsed(2 * DiskBalancerTestUtil.GB);
    Assert.assertEquals(v1.getUsed(),v1.getCapacity());
    // If usage is less than capacity, usage should be set to the real usage
    DiskBalancerVolume v2 = util.createRandomVolume(StorageType.DISK);
    v2.setCapacity(2*DiskBalancerTestUtil.GB);
    v2.setUsed(DiskBalancerTestUtil.GB);
    Assert.assertEquals(v1.getUsed(),DiskBalancerTestUtil.GB);
  }
}

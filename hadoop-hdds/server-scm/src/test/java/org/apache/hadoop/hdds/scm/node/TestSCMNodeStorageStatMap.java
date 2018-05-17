/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMNodeReport;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMStorageReport;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TestSCMNodeStorageStatMap {
  private final static int DATANODE_COUNT = 300;
  final long capacity = 10L * OzoneConsts.GB;
  final long used = 2L * OzoneConsts.GB;
  final long remaining = capacity - used;
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private final Map<UUID, SCMNodeStat> testData = new ConcurrentHashMap<>();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private void generateData() {
    SCMNodeStat stat = new SCMNodeStat();
    stat.set(capacity, used, remaining);
    for (int dnIndex = 1; dnIndex <= DATANODE_COUNT; dnIndex++) {
      testData.put(UUID.randomUUID(), stat);
    }
  }

  private UUID getFirstKey() {
    return testData.keySet().iterator().next();
  }

  @Before
  public void setUp() throws Exception {
    generateData();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testIsKnownDatanode() throws SCMException {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    UUID knownNode = getFirstKey();
    UUID unknownNode = UUID.randomUUID();
    SCMNodeStat stat = testData.get(knownNode);
    map.insertNewDatanode(knownNode, stat);
    Assert.assertTrue("Not able to detect a known node",
        map.isKnownDatanode(knownNode));
    Assert.assertFalse("Unknown node detected",
        map.isKnownDatanode(unknownNode));
  }

  @Test
  public void testInsertNewDatanode() throws SCMException {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    UUID knownNode = getFirstKey();
    SCMNodeStat stat = testData.get(knownNode);
    map.insertNewDatanode(knownNode, stat);
    Assert.assertEquals(map.getNodeStat(knownNode).getScmUsed(),
        testData.get(knownNode).getScmUsed());
    thrown.expect(SCMException.class);
    thrown.expectMessage("already exists");
    map.insertNewDatanode(knownNode, stat);
  }

  @Test
  public void testUpdateUnknownDatanode() throws SCMException {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    UUID unknownNode = UUID.randomUUID();
    SCMNodeStat stat = new SCMNodeStat();

    thrown.expect(SCMException.class);
    thrown.expectMessage("No such datanode");
    map.updateDatanodeMap(unknownNode, stat);
  }

  @Test
  public void testProcessNodeReportCheckOneNode() throws SCMException {
    UUID key = getFirstKey();
    SCMNodeStat value = testData.get(key);
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    map.insertNewDatanode(key, value);
    Assert.assertTrue(map.isKnownDatanode(key));
    SCMNodeReport.Builder nrb = SCMNodeReport.newBuilder();
    SCMStorageReport.Builder srb = SCMStorageReport.newBuilder();
    srb.setStorageUuid(UUID.randomUUID().toString());
    srb.setCapacity(value.getCapacity().get())
        .setScmUsed(value.getScmUsed().get()).
        setRemaining(value.getRemaining().get()).build();
    SCMNodeStorageStatMap.NodeReportStatus status =
        map.processNodeReport(key, nrb.addStorageReport(srb).build());
    Assert.assertEquals(status,
        SCMNodeStorageStatMap.NodeReportStatus.ALL_IS_WELL);
  }

  @Test
  public void testProcessNodeReportAndSCMStats() throws SCMException {
    SCMNodeStorageStatMap map = new SCMNodeStorageStatMap(conf);
    int counter = 1;
    // Insert all testData into the SCMNodeStorageStatMap Map.
    for (Map.Entry<UUID, SCMNodeStat> keyEntry : testData.entrySet()) {
      map.insertNewDatanode(keyEntry.getKey(), keyEntry.getValue());
    }
    Assert.assertEquals(DATANODE_COUNT * capacity, map.getTotalCapacity());
    Assert.assertEquals(DATANODE_COUNT * remaining, map.getTotalFreeSpace());
    Assert.assertEquals(DATANODE_COUNT * used, map.getTotalSpaceUsed());

    // upadate 1/4th of the datanode to be full
    for (Map.Entry<UUID, SCMNodeStat> keyEntry : testData.entrySet()) {
      SCMNodeStat stat = new SCMNodeStat(capacity, capacity, 0);
      map.updateDatanodeMap(keyEntry.getKey(), stat);
      counter++;
      if (counter > DATANODE_COUNT / 4) {
        break;
      }
    }
    Assert.assertEquals(DATANODE_COUNT / 4,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.CRITICAL)
            .size());
    Assert.assertEquals(0,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.WARN)
            .size());
    Assert.assertEquals(0.75 * DATANODE_COUNT,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.NORMAL)
            .size(), 0);

    Assert.assertEquals(DATANODE_COUNT * capacity, map.getTotalCapacity(), 0);
    Assert.assertEquals(0.75 * DATANODE_COUNT * remaining,
        map.getTotalFreeSpace(), 0);
    Assert.assertEquals(
        0.75 * DATANODE_COUNT * used + (0.25 * DATANODE_COUNT * capacity),
        map.getTotalSpaceUsed(), 0);
    counter = 1;
    // Remove 1/4 of the DataNodes from the Map
    for (Map.Entry<UUID, SCMNodeStat> keyEntry : testData.entrySet()) {
      map.removeDatanode(keyEntry.getKey());
      counter++;
      if (counter > DATANODE_COUNT / 4) {
        break;
      }
    }

    Assert.assertEquals(0,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.CRITICAL)
            .size());
    Assert.assertEquals(0,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.WARN)
            .size());
    Assert.assertEquals(0.75 * DATANODE_COUNT,
        map.getDatanodeList(SCMNodeStorageStatMap.UtilizationThreshold.NORMAL)
            .size(), 0);

    Assert.assertEquals(0.75 * DATANODE_COUNT * capacity, map.getTotalCapacity(), 0);
    Assert.assertEquals(0.75 * DATANODE_COUNT * remaining,
        map.getTotalFreeSpace(), 0);
    Assert.assertEquals(
        0.75 * DATANODE_COUNT * used ,
        map.getTotalSpaceUsed(), 0);

  }
}

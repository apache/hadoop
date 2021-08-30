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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.test.Whitebox;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TestHostFileManager {
  private static InetSocketAddress entry(String e) {
    return HostFileManager.parseEntry("dummy", "dummy", e);
  }

  @Test
  public void testDeduplication() {
    HostSet s = new HostSet();
    // These entries will be de-duped, since they refer to the same IP
    // address + port combo.
    s.add(entry("127.0.0.1:12345"));
    s.add(entry("localhost:12345"));
    Assertions.assertEquals(1, s.size());
    s.add(entry("127.0.0.1:12345"));
    Assertions.assertEquals(1, s.size());

    // The following entries should not be de-duped.
    s.add(entry("127.0.0.1:12346"));
    Assertions.assertEquals(2, s.size());
    s.add(entry("127.0.0.1"));
    Assertions.assertEquals(3, s.size());
    s.add(entry("127.0.0.10"));
    Assertions.assertEquals(4, s.size());
  }

  @Test
  public void testRelation() {
    HostSet s = new HostSet();
    s.add(entry("127.0.0.1:123"));
    Assertions.assertTrue(s.match(entry("127.0.0.1:123")));
    Assertions.assertFalse(s.match(entry("127.0.0.1:12")));
    Assertions.assertFalse(s.match(entry("127.0.0.1")));
    Assertions.assertFalse(s.matchedBy(entry("127.0.0.1:12")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.1")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.1:123")));
    Assertions.assertFalse(s.match(entry("127.0.0.2")));
    Assertions.assertFalse(s.match(entry("127.0.0.2:123")));
    Assertions.assertFalse(s.matchedBy(entry("127.0.0.2")));
    Assertions.assertFalse(s.matchedBy(entry("127.0.0.2:123")));

    s.add(entry("127.0.0.1"));
    Assertions.assertTrue(s.match(entry("127.0.0.1:123")));
    Assertions.assertTrue(s.match(entry("127.0.0.1:12")));
    Assertions.assertTrue(s.match(entry("127.0.0.1")));
    Assertions.assertFalse(s.matchedBy(entry("127.0.0.1:12")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.1")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.1:123")));
    Assertions.assertFalse(s.match(entry("127.0.0.2")));
    Assertions.assertFalse(s.match(entry("127.0.0.2:123")));
    Assertions.assertFalse(s.matchedBy(entry("127.0.0.2")));
    Assertions.assertFalse(s.matchedBy(entry("127.0.0.2:123")));

    s.add(entry("127.0.0.2:123"));
    Assertions.assertTrue(s.match(entry("127.0.0.1:123")));
    Assertions.assertTrue(s.match(entry("127.0.0.1:12")));
    Assertions.assertTrue(s.match(entry("127.0.0.1")));
    Assertions.assertFalse(s.matchedBy(entry("127.0.0.1:12")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.1")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.1:123")));
    Assertions.assertFalse(s.match(entry("127.0.0.2")));
    Assertions.assertTrue(s.match(entry("127.0.0.2:123")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.2")));
    Assertions.assertTrue(s.matchedBy(entry("127.0.0.2:123")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncludeExcludeLists() throws IOException {
    BlockManager bm = mock(BlockManager.class);
    FSNamesystem fsn = mock(FSNamesystem.class);
    Configuration conf = new Configuration();
    HostFileManager hm = new HostFileManager();
    HostSet includedNodes = new HostSet();
    HostSet excludedNodes = new HostSet();

    includedNodes.add(entry("127.0.0.1:12345"));
    includedNodes.add(entry("localhost:12345"));
    includedNodes.add(entry("127.0.0.1:12345"));
    includedNodes.add(entry("127.0.0.2"));

    excludedNodes.add(entry("127.0.0.1:12346"));
    excludedNodes.add(entry("127.0.30.1:12346"));

    Assertions.assertEquals(2, includedNodes.size());
    Assertions.assertEquals(2, excludedNodes.size());

    hm.refresh(includedNodes, excludedNodes);

    DatanodeManager dm = new DatanodeManager(bm, fsn, conf);
    Whitebox.setInternalState(dm, "hostConfigManager", hm);
    Map<String, DatanodeDescriptor> dnMap = (Map<String,
            DatanodeDescriptor>) Whitebox.getInternalState(dm, "datanodeMap");

    // After the de-duplication, there should be only one DN from the included
    // nodes declared as dead.
    Assertions.assertEquals(2, dm.getDatanodeListForReport(HdfsConstants
            .DatanodeReportType.ALL).size());
    Assertions.assertEquals(2, dm.getDatanodeListForReport(HdfsConstants
            .DatanodeReportType.DEAD).size());
    dnMap.put("uuid-foo", new DatanodeDescriptor(new DatanodeID("127.0.0.1",
            "localhost", "uuid-foo", 12345, 1020, 1021, 1022)));
    Assertions.assertEquals(1, dm.getDatanodeListForReport(HdfsConstants
            .DatanodeReportType.DEAD).size());
    dnMap.put("uuid-bar", new DatanodeDescriptor(new DatanodeID("127.0.0.2",
            "127.0.0.2", "uuid-bar", 12345, 1020, 1021, 1022)));
    Assertions.assertEquals(0, dm.getDatanodeListForReport(HdfsConstants
            .DatanodeReportType.DEAD).size());
    DatanodeDescriptor spam = new DatanodeDescriptor(new DatanodeID("127.0.0" +
            ".3", "127.0.0.3", "uuid-spam", 12345, 1020, 1021, 1022));
    DFSTestUtil.setDatanodeDead(spam);
    includedNodes.add(entry("127.0.0.3:12345"));
    dnMap.put("uuid-spam", spam);
    Assertions.assertEquals(1, dm.getDatanodeListForReport(HdfsConstants
            .DatanodeReportType.DEAD).size());
    dnMap.remove("uuid-spam");
    Assertions.assertEquals(1, dm.getDatanodeListForReport(HdfsConstants
            .DatanodeReportType.DEAD).size());
    excludedNodes.add(entry("127.0.0.3"));
    Assertions.assertEquals(1, dm.getDatanodeListForReport(HdfsConstants
            .DatanodeReportType.DEAD).size());
  }
}

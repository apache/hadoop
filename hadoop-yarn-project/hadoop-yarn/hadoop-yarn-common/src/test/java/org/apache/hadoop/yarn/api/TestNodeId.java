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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.NodeId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNodeId {
  @Test
  void testNodeId() {
    NodeId nodeId1 = NodeId.newInstance("10.18.52.124", 8041);
    NodeId nodeId2 = NodeId.newInstance("10.18.52.125", 8038);
    NodeId nodeId3 = NodeId.newInstance("10.18.52.124", 8041);
    NodeId nodeId4 = NodeId.newInstance("10.18.52.124", 8039);

    assertEquals(nodeId1, nodeId3);
    assertNotEquals(nodeId1, nodeId2);
    assertNotEquals(nodeId3, nodeId4);

    assertTrue(nodeId1.compareTo(nodeId3) == 0);
    assertTrue(nodeId1.compareTo(nodeId2) < 0);
    assertTrue(nodeId3.compareTo(nodeId4) > 0);

    assertTrue(nodeId1.hashCode() == nodeId3.hashCode());
    assertFalse(nodeId1.hashCode() == nodeId2.hashCode());
    assertFalse(nodeId3.hashCode() == nodeId4.hashCode());

    assertEquals("10.18.52.124:8041", nodeId1.toString());
  }

}

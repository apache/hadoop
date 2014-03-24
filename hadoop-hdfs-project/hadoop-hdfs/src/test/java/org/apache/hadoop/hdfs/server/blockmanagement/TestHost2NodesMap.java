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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.Before;
import org.junit.Test;

public class TestHost2NodesMap {
  private final Host2NodesMap map = new Host2NodesMap();
  private DatanodeDescriptor dataNodes[];
  
  @Before
  public void setup() {
    dataNodes = new DatanodeDescriptor[] {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r2"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", 5021, "/d1/r2"),
    };
    for (DatanodeDescriptor node : dataNodes) {
      map.add(node);
    }
    map.add(null);
  }
  
  @Test
  public void testContains() throws Exception {
    DatanodeDescriptor nodeNotInMap =
      DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r4");
    for (int i = 0; i < dataNodes.length; i++) {
      assertTrue(map.contains(dataNodes[i]));
    }
    assertFalse(map.contains(null));
    assertFalse(map.contains(nodeNotInMap));
  }

  @Test
  public void testGetDatanodeByHost() throws Exception {
    assertEquals(map.getDatanodeByHost("1.1.1.1"), dataNodes[0]);
    assertEquals(map.getDatanodeByHost("2.2.2.2"), dataNodes[1]);
    DatanodeDescriptor node = map.getDatanodeByHost("3.3.3.3");
    assertTrue(node == dataNodes[2] || node == dataNodes[3]);
    assertNull(map.getDatanodeByHost("4.4.4.4"));
  }

  @Test
  public void testRemove() throws Exception {
    DatanodeDescriptor nodeNotInMap =
      DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r4");
    assertFalse(map.remove(nodeNotInMap));
    
    assertTrue(map.remove(dataNodes[0]));
    assertTrue(map.getDatanodeByHost("1.1.1.1.")==null);
    assertTrue(map.getDatanodeByHost("2.2.2.2")==dataNodes[1]);
    DatanodeDescriptor node = map.getDatanodeByHost("3.3.3.3");
    assertTrue(node==dataNodes[2] || node==dataNodes[3]);
    assertNull(map.getDatanodeByHost("4.4.4.4"));
    
    assertTrue(map.remove(dataNodes[2]));
    assertNull(map.getDatanodeByHost("1.1.1.1"));
    assertEquals(map.getDatanodeByHost("2.2.2.2"), dataNodes[1]);
    assertEquals(map.getDatanodeByHost("3.3.3.3"), dataNodes[3]);
    
    assertTrue(map.remove(dataNodes[3]));
    assertNull(map.getDatanodeByHost("1.1.1.1"));
    assertEquals(map.getDatanodeByHost("2.2.2.2"), dataNodes[1]);
    assertNull(map.getDatanodeByHost("3.3.3.3"));
    
    assertFalse(map.remove(null));
    assertTrue(map.remove(dataNodes[1]));
    assertFalse(map.remove(dataNodes[1]));
  }

}

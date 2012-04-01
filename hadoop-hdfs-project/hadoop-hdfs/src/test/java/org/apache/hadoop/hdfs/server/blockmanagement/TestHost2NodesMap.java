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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.junit.Before;
import org.junit.Test;

public class TestHost2NodesMap {
  private Host2NodesMap map = new Host2NodesMap();
  private final DatanodeDescriptor dataNodes[] = new DatanodeDescriptor[] {
    new DatanodeDescriptor(new DatanodeID("ip1", "h1", "", 5020, -1, -1), "/d1/r1"),
    new DatanodeDescriptor(new DatanodeID("ip2", "h1", "", 5020, -1, -1), "/d1/r1"),
    new DatanodeDescriptor(new DatanodeID("ip3", "h1", "", 5020, -1, -1), "/d1/r2"),
    new DatanodeDescriptor(new DatanodeID("ip3", "h1", "", 5030, -1, -1), "/d1/r2"),
  };
  private final DatanodeDescriptor NULL_NODE = null; 
  private final DatanodeDescriptor NODE = new DatanodeDescriptor(new DatanodeID("h3", 5040),
      "/d1/r4");

  @Before
  public void setup() {
    for(DatanodeDescriptor node:dataNodes) {
      map.add(node);
    }
    map.add(NULL_NODE);
  }
  
  @Test
  public void testContains() throws Exception {
    for(int i=0; i<dataNodes.length; i++) {
      assertTrue(map.contains(dataNodes[i]));
    }
    assertFalse(map.contains(NULL_NODE));
    assertFalse(map.contains(NODE));
  }

  @Test
  public void testGetDatanodeByHost() throws Exception {
    assertTrue(map.getDatanodeByHost("ip1")==dataNodes[0]);
    assertTrue(map.getDatanodeByHost("ip2")==dataNodes[1]);
    DatanodeDescriptor node = map.getDatanodeByHost("ip3");
    assertTrue(node==dataNodes[2] || node==dataNodes[3]);
    assertTrue(null==map.getDatanodeByHost("ip4"));
  }

  @Test
  public void testRemove() throws Exception {
    assertFalse(map.remove(NODE));
    
    assertTrue(map.remove(dataNodes[0]));
    assertTrue(map.getDatanodeByHost("ip1")==null);
    assertTrue(map.getDatanodeByHost("ip2")==dataNodes[1]);
    DatanodeDescriptor node = map.getDatanodeByHost("ip3");
    assertTrue(node==dataNodes[2] || node==dataNodes[3]);
    assertTrue(null==map.getDatanodeByHost("ip4"));
    
    assertTrue(map.remove(dataNodes[2]));
    assertTrue(map.getDatanodeByHost("ip1")==null);
    assertTrue(map.getDatanodeByHost("ip2")==dataNodes[1]);
    assertTrue(map.getDatanodeByHost("ip3")==dataNodes[3]);
    
    assertTrue(map.remove(dataNodes[3]));
    assertTrue(map.getDatanodeByHost("ip1")==null);
    assertTrue(map.getDatanodeByHost("ip2")==dataNodes[1]);
    assertTrue(map.getDatanodeByHost("ip3")==null);
    
    assertFalse(map.remove(NULL_NODE));
    assertTrue(map.remove(dataNodes[1]));
    assertFalse(map.remove(dataNodes[1]));
  }

}

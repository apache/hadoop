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

package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test the static mapping class.
 * Because the map is actually static, this map needs to be reset for every test
 */
public class TestStaticMapping extends Assert {

  /**
   * Reset the map then create a new instance of the {@link StaticMapping}
   * class
   * @return a new instance
   */
  private StaticMapping newInstance() {
    StaticMapping.resetMap();
    return new StaticMapping();
  }

  @Test
  public void testStaticIsSingleSwitch() throws Throwable {
    StaticMapping mapping = newInstance();
    assertTrue("Empty maps are not single switch", mapping.isSingleSwitch());
  }


  @Test
  public void testCachingRelaysQueries() throws Throwable {
    StaticMapping staticMapping = newInstance();
    CachedDNSToSwitchMapping mapping =
        new CachedDNSToSwitchMapping(staticMapping);
    assertTrue("Expected single switch", mapping.isSingleSwitch());
    StaticMapping.addNodeToRack("n1", "r1");
    assertFalse("Expected to be multi switch",
                mapping.isSingleSwitch());
  }

  @Test
  public void testAddResolveNodes() throws Throwable {
    StaticMapping mapping = newInstance();
    StaticMapping.addNodeToRack("n1", "r1");
    List<String> l1 = new ArrayList<String>(2);
    l1.add("n1");
    l1.add("unknown");
    List<String> mappings = mapping.resolve(l1);
    assertEquals(2, mappings.size());
    assertEquals("r1", mappings.get(0));
    assertEquals(NetworkTopology.DEFAULT_RACK, mappings.get(1));
    assertFalse("Mapping is still single switch", mapping.isSingleSwitch());
  }

  @Test
  public void testReadNodesFromConfig() throws Throwable {
    StaticMapping mapping = newInstance();
    Configuration conf = new Configuration();
    conf.set(StaticMapping.KEY_HADOOP_CONFIGURED_NODE_MAPPING, "n1=r1,n2=r2");
    mapping.setConf(conf);
    List<String> l1 = new ArrayList<String>(3);
    l1.add("n1");
    l1.add("unknown");
    l1.add("n2");
    List<String> mappings = mapping.resolve(l1);
    assertEquals(3, mappings.size());
    assertEquals("r1", mappings.get(0));
    assertEquals(NetworkTopology.DEFAULT_RACK, mappings.get(1));
    assertEquals("r2", mappings.get(2));
    assertFalse("Expected to be multi switch",
                AbstractDNSToSwitchMapping.isMappingSingleSwitch(mapping));
  }

  @Test
  public void testNullConfiguration() throws Throwable {
    StaticMapping mapping = newInstance();
    mapping.setConf(null);
    assertTrue("Null maps is not single switch", mapping.isSingleSwitch());
    assertTrue("Expected to be single switch",
               AbstractDNSToSwitchMapping.isMappingSingleSwitch(mapping));
  }
}

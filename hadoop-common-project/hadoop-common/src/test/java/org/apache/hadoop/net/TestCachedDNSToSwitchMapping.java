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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestCachedDNSToSwitchMapping {

  @Test
  public void testReloadCachedMappings() {
    StaticMapping.resetMap();
    StaticMapping.addNodeToRack("127.0.0.1", "/rack0");
    StaticMapping.addNodeToRack("notexisit.host.com", "/rack1");
    CachedDNSToSwitchMapping cacheMapping =
        new CachedDNSToSwitchMapping(new StaticMapping());
    List<String> names = new ArrayList<>();
    names.add("localhost");
    names.add("notexisit.host.com");
    cacheMapping.resolve(names);
    Assert.assertTrue(cacheMapping.getSwitchMap().containsKey("127.0.0.1"));
    Assert.assertTrue(cacheMapping.getSwitchMap().containsKey("notexisit.host.com"));
    cacheMapping.reloadCachedMappings(names);
    Assert.assertEquals(0, cacheMapping.getSwitchMap().keySet().size());
  }
}

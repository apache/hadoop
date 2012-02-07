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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.Node;
import org.junit.Assert;
import org.junit.Test;

public class TestRackResolver {

  public static final class MyResolver implements DNSToSwitchMapping {

    int numHost1 = 0;

    @Override
    public List<String> resolve(List<String> hostList) {
      // Only one host at a time
      Assert.assertTrue("hostList size is " + hostList.size(),
        hostList.size() <= 1);
      List<String> returnList = new ArrayList<String>();
      if (hostList.isEmpty()) {
        return returnList;
      }
      if (hostList.get(0).equals("host1")) {
        numHost1++;
        returnList.add("/rack1");
      }
      // I should not be reached again as RackResolver is supposed to do
      // caching.
      Assert.assertTrue(numHost1 <= 1);
      return returnList;
    }
    
  }

  @Test
  public void testCaching() {
    Configuration conf = new Configuration();
    conf.setClass(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
      MyResolver.class, DNSToSwitchMapping.class);
    RackResolver.init(conf);
    Node node = RackResolver.resolve("host1");
    Assert.assertEquals("/rack1", node.getNetworkLocation());
    node = RackResolver.resolve("host1");
    Assert.assertEquals("/rack1", node.getNetworkLocation());
  }

}

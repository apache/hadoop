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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRackResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRackResolver.class);
  private static final String invalidHost = "invalidHost";

  @Before
  public void setUp() {
    RackResolver.reset();
  }

  public static final class MyResolver implements DNSToSwitchMapping {

    int numHost1 = 0;
    public static String resolvedHost1 = "host1";

    @Override
    public List<String> resolve(List<String> hostList) {
      // Only one host at a time
      Assert.assertTrue("hostList size is " + hostList.size(),
        hostList.size() <= 1);
      List<String> returnList = new ArrayList<String>();
      if (hostList.isEmpty()) {
        return returnList;
      }
      if (hostList.get(0).equals(invalidHost)) {
        // Simulate condition where resolving host returns null
        return null; 
      }
        
      LOG.info("Received resolve request for "
          + hostList.get(0));
      if (hostList.get(0).equals("host1")
          || hostList.get(0).equals(resolvedHost1)) {
        numHost1++;
        returnList.add("/rack1");
      }
      // I should not be reached again as RackResolver is supposed to do
      // caching.
      Assert.assertTrue(numHost1 <= 1);
      return returnList;
    }

    @Override
    public void reloadCachedMappings() {
      // nothing to do here, since RawScriptBasedMapping has no cache.
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
  }

  /**
   * This class is to test the resolve method which accepts a list of hosts
   * in RackResolver.
   */
  public static final class MultipleResolver implements DNSToSwitchMapping {

    @Override
    public List<String> resolve(List<String> hostList) {
      List<String> returnList = new ArrayList<String>();
      if (hostList.isEmpty()) {
        return returnList;
      }
      for (String host : hostList) {
        if (host.equals(invalidHost)) {
          // Simulate condition where resolving host returns empty string
          returnList.add("");
        }
        LOG.info("Received resolve request for " + host);
        if (host.startsWith("host")) {
          returnList.add("/" + host.replace("host", "rack"));
        }
        // I should not be reached again as RackResolver is supposed to do
        // caching.
      }
      Assert.assertEquals(returnList.size(), hostList.size());
      return returnList;
    }

    @Override
    public void reloadCachedMappings() {
      // nothing to do here, since RawScriptBasedMapping has no cache.
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
  }

  @Test
  public void testCaching() {
    Configuration conf = new Configuration();
    conf.setClass(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
      MyResolver.class, DNSToSwitchMapping.class);
    RackResolver.init(conf);
    try {
      InetAddress iaddr = InetAddress.getByName("host1");
      MyResolver.resolvedHost1 = iaddr.getHostAddress();
    } catch (UnknownHostException e) {
      // Ignore if not found
    }
    Node node = RackResolver.resolve("host1");
    Assert.assertEquals("/rack1", node.getNetworkLocation());
    node = RackResolver.resolve("host1");
    Assert.assertEquals("/rack1", node.getNetworkLocation());
    node = RackResolver.resolve(invalidHost);
    Assert.assertEquals(NetworkTopology.DEFAULT_RACK, node.getNetworkLocation());
  }

  @Test
  public void testMultipleHosts() {
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic
            .NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MultipleResolver.class,
        DNSToSwitchMapping.class);
    RackResolver.init(conf);
    List<Node> nodes = RackResolver.resolve(
        Arrays.asList("host1", invalidHost, "host2"));
    Assert.assertEquals("/rack1", nodes.get(0).getNetworkLocation());
    Assert.assertEquals(NetworkTopology.DEFAULT_RACK,
        nodes.get(1).getNetworkLocation());
    Assert.assertEquals("/rack2", nodes.get(2).getNetworkLocation());
  }
}

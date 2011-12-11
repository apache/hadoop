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
package org.apache.hadoop.mapred;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.ClusterTopologyReader;
import org.apache.hadoop.tools.rumen.DeepInequalityException;
import org.apache.hadoop.tools.rumen.LoggedNetworkTopology;
import org.apache.hadoop.tools.rumen.TreePath;
import org.junit.Test;

public class TestRemoveIpsFromLoggedNetworkTopology {
  
  
  @Test
  public void testIsIPAddress() {
    final String[] positives = {
        "123.13.42.255", // regular ipv4
        "123.01.0.255", // padded 0
        "000.001.002.020", // more padded 0
        "123\\.13\\.42\\.255", // escaped .
        "0.0.0.0", // all-zero
        "255.255.255.255", // all-0xff
        
        "1080:0:0:0:8:800:200C:417A", // regular ipv6
        "1080:01:020:3:8:0800:200C:417A", // padded 0
        "1080:01:002:0003:080:0800:0200:417A", // more padded 0
        "0:0:0:0:0:0:0:0", // all-zero
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", // all-0xff
    };

    final String[] negatives = {
        "node.megatron.com", // domain name
        "13.42.255", // too short
        "123.13.42.255.10", // too long
        "123.256.42.255", // too large
        "123.13.42.255.weird.com", // weird
        "1080:0:0:0:8:200C:417A", // too short
        "1080:0:0:0:1:8:800:200C:417A", // too long
        "1080A:0:0:0:8:800:200C:417A", // too large
        "1080:0:0:0:8:800:200G:417A", // too large
    };
    
    for (String s : positives) {
      Assert.assertTrue(s, SimulatorEngine.isIPAddress(s));
    }
    
    for (String s : negatives) {
      Assert.assertFalse(s, SimulatorEngine.isIPAddress(s));
    }
  }
  
  @Test
  public void testIpRemoval() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);
    final Path rootInputDir = new Path(System.getProperty("src.test.data",
        "data")).makeQualified(lfs.getUri(), lfs.getWorkingDirectory());

    final LoggedNetworkTopology topoWithIps = new ClusterTopologyReader(new Path(
        rootInputDir, "topo-with-numeric-ips.json"), conf).get();
    final LoggedNetworkTopology topoWithoutIps = new ClusterTopologyReader(new Path(
        rootInputDir, "topo-without-numeric-ips.json"), conf).get();
    try {
      topoWithIps.deepCompare(topoWithoutIps, new TreePath(null, "<root>"));
      Assert.fail("Expecting two topologies to differ");
    } catch (DeepInequalityException e) {
    }
    SimulatorEngine.removeIpHosts(topoWithIps);
    try {
      topoWithIps.deepCompare(topoWithoutIps, new TreePath(null, "<root>"));
    } catch (DeepInequalityException e) {
      Assert.fail("Expecting two topologies to be equal");
    }
  }
}

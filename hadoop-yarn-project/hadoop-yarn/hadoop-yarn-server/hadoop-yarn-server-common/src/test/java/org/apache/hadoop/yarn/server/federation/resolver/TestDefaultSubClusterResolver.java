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

package org.apache.hadoop.yarn.server.federation.resolver;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link SubClusterResolver} against correct and malformed Federation
 * machine lists.
 */
public class TestDefaultSubClusterResolver {
  private static YarnConfiguration conf;
  private static SubClusterResolver resolver;

  public static void setUpGoodFile() {
    conf = new YarnConfiguration();
    resolver = new DefaultSubClusterResolverImpl();

    URL url =
        Thread.currentThread().getContextClassLoader().getResource("nodes");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'nodes' dummy file in classpath");
    }
    // This will get rid of the beginning '/' in the url in Windows env
    File file = new File(url.getPath());

    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, file.getPath());
    resolver.setConf(conf);
    resolver.load();
  }

  private void setUpMalformedFile() {
    conf = new YarnConfiguration();
    resolver = new DefaultSubClusterResolverImpl();

    URL url = Thread.currentThread().getContextClassLoader()
        .getResource("nodes-malformed");
    if (url == null) {
      throw new RuntimeException(
          "Could not find 'nodes-malformed' dummy file in classpath");
    }
    // This will get rid of the beginning '/' in the url in Windows env
    File file = new File(url.getPath());

    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, file.getPath());
    resolver.setConf(conf);
    resolver.load();
  }

  private void setUpNonExistentFile() {
    conf = new YarnConfiguration();
    resolver = new DefaultSubClusterResolverImpl();

    conf.set(YarnConfiguration.FEDERATION_MACHINE_LIST, "fileDoesNotExist");
    resolver.setConf(conf);
    resolver.load();
  }

  @Test
  public void testGetSubClusterForNode() throws YarnException {
    setUpGoodFile();

    // All lowercase, no whitespace in machine list file
    Assert.assertEquals(SubClusterId.newInstance("subcluster1"),
        resolver.getSubClusterForNode("node1"));
    // Leading and trailing whitespace in machine list file
    Assert.assertEquals(SubClusterId.newInstance("subcluster2"),
        resolver.getSubClusterForNode("node2"));
    // Node name capitalization in machine list file
    Assert.assertEquals(SubClusterId.newInstance("subcluster3"),
        resolver.getSubClusterForNode("node3"));

    try {
      resolver.getSubClusterForNode("nodeDoesNotExist");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }
  }

  @Test
  public void testGetSubClusterForNodeMalformedFile() throws YarnException {
    setUpMalformedFile();

    try {
      resolver.getSubClusterForNode("node1");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }

    try {
      resolver.getSubClusterForNode("node2");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }

    Assert.assertEquals(SubClusterId.newInstance("subcluster3"),
        resolver.getSubClusterForNode("node3"));

    try {
      resolver.getSubClusterForNode("nodeDoesNotExist");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }
  }

  @Test
  public void testGetSubClusterForNodeNoFile() throws YarnException {
    setUpNonExistentFile();

    try {
      resolver.getSubClusterForNode("node1");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(
          e.getMessage().startsWith("Cannot find subClusterId for node"));
    }
  }

  @Test
  public void testGetSubClustersForRack() throws YarnException {
    setUpGoodFile();

    Set<SubClusterId> rack1Expected = new HashSet<SubClusterId>();
    rack1Expected.add(SubClusterId.newInstance("subcluster1"));
    rack1Expected.add(SubClusterId.newInstance("subcluster2"));

    Set<SubClusterId> rack2Expected = new HashSet<SubClusterId>();
    rack2Expected.add(SubClusterId.newInstance("subcluster3"));

    // Two subclusters have nodes in rack1
    Assert.assertEquals(rack1Expected, resolver.getSubClustersForRack("rack1"));

    // Two nodes are in rack2, but both belong to subcluster3
    Assert.assertEquals(rack2Expected, resolver.getSubClustersForRack("rack2"));

    try {
      resolver.getSubClustersForRack("rackDoesNotExist");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().startsWith("Cannot resolve rack"));
    }
  }

  @Test
  public void testGetSubClustersForRackNoFile() throws YarnException {
    setUpNonExistentFile();

    try {
      resolver.getSubClustersForRack("rack1");
      Assert.fail();
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().startsWith("Cannot resolve rack"));
    }
  }
}

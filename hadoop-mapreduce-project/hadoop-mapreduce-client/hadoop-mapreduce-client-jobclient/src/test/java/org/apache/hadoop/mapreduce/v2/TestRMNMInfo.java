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

package org.apache.hadoop.mapreduce.v2;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMNMInfo;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class TestRMNMInfo {
  private static final Log LOG = LogFactory.getLog(TestRMNMInfo.class);
  private static final int NUMNODEMANAGERS = 4;
  protected static MiniMRYarnCluster mrCluster;

  private static Configuration initialConf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(initialConf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }
  private static Path TEST_ROOT_DIR =
         new Path("target",TestRMNMInfo.class.getName() + "-tmpDir")
              .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");

  @BeforeClass
  public static void setup() throws IOException {

    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
               + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniMRYarnCluster(TestRMNMInfo.class.getName(),
                                        NUMNODEMANAGERS);
      Configuration conf = new Configuration();
      mrCluster.init(conf);
      mrCluster.start();
    }

    // workaround the absent public distcache.
    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    localFs.setPermission(APP_JAR, new FsPermission("700"));
  }

  @AfterClass
  public static void tearDown() {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
  }

  @Test
  public void testRMNMInfo() throws Exception {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
           + " not found. Not running test.");
      return;
    }
    
    RMContext rmc = mrCluster.getResourceManager().getRMContext();
    ResourceScheduler rms = mrCluster.getResourceManager()
                                                   .getResourceScheduler();
    RMNMInfo rmInfo = new RMNMInfo(rmc,rms);
    String liveNMs = rmInfo.getLiveNodeManagers();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jn = mapper.readTree(liveNMs);
    Assert.assertEquals("Unexpected number of live nodes:",
                                               NUMNODEMANAGERS, jn.size());
    Iterator<JsonNode> it = jn.iterator();
    while (it.hasNext()) {
      JsonNode n = it.next();
      Assert.assertNotNull(n.get("HostName"));
      Assert.assertNotNull(n.get("Rack"));
      Assert.assertTrue("Node " + n.get("NodeId") + " should be RUNNING",
              n.get("State").getValueAsText().contains("RUNNING"));
      Assert.assertNotNull(n.get("NodeHTTPAddress"));
      Assert.assertNotNull(n.get("LastHealthUpdate"));
      Assert.assertNotNull(n.get("HealthReport"));
      Assert.assertNotNull(n.get("NodeManagerVersion"));
      Assert.assertNotNull(n.get("NumContainers"));
      Assert.assertEquals(
              n.get("NodeId") + ": Unexpected number of used containers",
              0, n.get("NumContainers").getValueAsInt());
      Assert.assertEquals(
              n.get("NodeId") + ": Unexpected amount of used memory",
              0, n.get("UsedMemoryMB").getValueAsInt());
      Assert.assertNotNull(n.get("AvailableMemoryMB"));
    }
  }
  
  @Test
  public void testRMNMInfoMissmatch() throws Exception {
    RMContext rmc = mock(RMContext.class);
    ResourceScheduler rms = mock(ResourceScheduler.class);
    ConcurrentMap<NodeId, RMNode> map = new ConcurrentHashMap<NodeId, RMNode>();
    RMNode node = MockNodes.newNodeInfo(1, MockNodes.newResource(4 * 1024));
    map.put(node.getNodeID(), node);
    when(rmc.getRMNodes()).thenReturn(map);
    
    RMNMInfo rmInfo = new RMNMInfo(rmc,rms);
    String liveNMs = rmInfo.getLiveNodeManagers();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jn = mapper.readTree(liveNMs);
    Assert.assertEquals("Unexpected number of live nodes:",
                                               1, jn.size());
    Iterator<JsonNode> it = jn.iterator();
    while (it.hasNext()) {
      JsonNode n = it.next();
      Assert.assertNotNull(n.get("HostName"));
      Assert.assertNotNull(n.get("Rack"));
      Assert.assertTrue("Node " + n.get("NodeId") + " should be RUNNING",
              n.get("State").getValueAsText().contains("RUNNING"));
      Assert.assertNotNull(n.get("NodeHTTPAddress"));
      Assert.assertNotNull(n.get("LastHealthUpdate"));
      Assert.assertNotNull(n.get("HealthReport"));
      Assert.assertNotNull(n.get("NodeManagerVersion"));
      Assert.assertNull(n.get("NumContainers"));
      Assert.assertNull(n.get("UsedMemoryMB"));
      Assert.assertNull(n.get("AvailableMemoryMB"));
    }
  }
}

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

package org.apache.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.BindException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRefreshCallQueue {
  private MiniDFSCluster cluster;
  private Configuration config;
  private FileSystem fs;
  static int mockQueueConstructions;
  static int mockQueuePuts;
  private String callQueueConfigKey = "";
  private final Random rand = new Random();

  @Before
  public void setUp() throws Exception {
    // We want to count additional events, so we reset here
    mockQueueConstructions = 0;
    mockQueuePuts = 0;
    int portRetries = 5;
    int nnPort;

    for (; portRetries > 0; --portRetries) {
      // Pick a random port in the range [30000,60000).
      nnPort = 30000 + rand.nextInt(30000);  
      config = new Configuration();
      callQueueConfigKey = "ipc." + nnPort + ".callqueue.impl";
      config.setClass(callQueueConfigKey,
          MockCallQueue.class, BlockingQueue.class);
      config.set("hadoop.security.authorization", "true");

      FileSystem.setDefaultUri(config, "hdfs://localhost:" + nnPort);
      fs = FileSystem.get(config);
      
      try {
        cluster = new MiniDFSCluster.Builder(config).nameNodePort(nnPort).build();
        cluster.waitActive();
        break;
      } catch (BindException be) {
        // Retry with a different port number.
      }
    }
    
    if (portRetries == 0) {
      // Bail if we get very unlucky with our choice of ports.
      fail("Failed to pick an ephemeral port for the NameNode RPC server.");
    }
  }

  @After
  public void tearDown() throws Exception {
    if(cluster!=null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @SuppressWarnings("serial")
  public static class MockCallQueue<E> extends LinkedBlockingQueue<E> {
    public MockCallQueue(int levels, int cap, String ns, Configuration conf) {
      super(cap);
      mockQueueConstructions++;
    }

    public void put(E e) throws InterruptedException {
      super.put(e);
      mockQueuePuts++;
    }
  }

  // Returns true if mock queue was used for put
  public boolean canPutInMockQueue() throws IOException {
  int putsBefore = mockQueuePuts;
  fs.exists(new Path("/")); // Make an RPC call
  return mockQueuePuts > putsBefore;
  }

  @Test
  public void testRefresh() throws Exception {
    assertTrue("Mock queue should have been constructed", mockQueueConstructions > 0);
    assertTrue("Puts are routed through MockQueue", canPutInMockQueue());
    int lastMockQueueConstructions = mockQueueConstructions;

    // Replace queue with the queue specified in core-site.xml, which would be the LinkedBlockingQueue
    DFSAdmin admin = new DFSAdmin(config);
    String [] args = new String[]{"-refreshCallQueue"};
    int exitCode = admin.run(args);
    assertEquals("DFSAdmin should return 0", 0, exitCode);

    assertEquals("Mock queue should have no additional constructions", lastMockQueueConstructions, mockQueueConstructions);
    try {
      assertFalse("Puts are routed through LBQ instead of MockQueue", canPutInMockQueue());
    } catch (IOException ioe){
      fail("Could not put into queue at all");
    }
  }

}
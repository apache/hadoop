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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

public class TestJobQueueClient {
  @Test
  public void testQueueOrdering() throws Exception {
    System.out.println("in test queue ordering");
    // create some sample queues in a hierarchy..
    JobQueueInfo[] roots = new JobQueueInfo[2];
    roots[0] = new JobQueueInfo("q1", "q1 scheduling info");
    roots[1] = new JobQueueInfo("q2", "q2 scheduling info");
    
    List<JobQueueInfo> children = new ArrayList<JobQueueInfo>();
    children.add(new JobQueueInfo("q1:1", null));
    children.add(new JobQueueInfo("q1:2", null));
    roots[0].setChildren(children);
    
    // test dfs ordering
    JobQueueClient client = new JobQueueClient(new JobConf());
    List<JobQueueInfo> allQueues = client.expandQueueList(roots);
    assertEquals(4, allQueues.size());
    assertEquals("q1", allQueues.get(0).getQueueName());
    assertEquals("q1:1", allQueues.get(1).getQueueName());
    assertEquals("q1:2", allQueues.get(2).getQueueName());
    assertEquals("q2", allQueues.get(3).getQueueName());
  }
  
  @Test
  public void testQueueInfoPrinting() throws Exception {
    System.out.println("in test queue info printing");
    // create a test queue with children.
    // create some sample queues in a hierarchy..
    JobQueueInfo root = new JobQueueInfo("q1", "q1 scheduling info");
    
    List<JobQueueInfo> children = new ArrayList<JobQueueInfo>();
    children.add(new JobQueueInfo("q1:1", null));
    children.add(new JobQueueInfo("q1:2", null));
    root.setChildren(children);

    JobQueueClient client = new JobQueueClient(new JobConf());
    StringWriter writer = new StringWriter();
    client.printJobQueueInfo(root, writer);
    
    StringBuffer sb = new StringBuffer();
    sb.append("Queue Name : q1 \n");
    sb.append("Queue State : running \n");
    sb.append("Scheduling Info : q1 scheduling info \n");
    sb.append("Child Queues : q1:1, q1:2\n");
    sb.append("======================\n");
    
    assertEquals(sb.toString(), writer.toString());
  }
}
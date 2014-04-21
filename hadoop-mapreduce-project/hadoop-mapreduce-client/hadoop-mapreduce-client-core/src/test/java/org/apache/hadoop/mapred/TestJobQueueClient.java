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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Assert;

import org.junit.Test;


public class TestJobQueueClient {
  /**
   * Test that print job queue recursively prints child queues
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testPrintJobQueueInfo() throws IOException {
    JobQueueClient queueClient = new JobQueueClient();
    JobQueueInfo parent = new JobQueueInfo();
    JobQueueInfo child = new JobQueueInfo();
    JobQueueInfo grandChild = new JobQueueInfo();
    child.addChild(grandChild);
    parent.addChild(child);
    grandChild.setQueueName("GrandChildQueue");

    ByteArrayOutputStream bbos = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(bbos);
    queueClient.printJobQueueInfo(parent, writer);

    Assert.assertTrue("printJobQueueInfo did not print grandchild's name",
      bbos.toString().contains("GrandChildQueue"));
  }

}

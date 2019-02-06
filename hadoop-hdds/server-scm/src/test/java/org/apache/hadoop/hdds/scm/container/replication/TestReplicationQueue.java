/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import java.util.Random;
import java.util.UUID;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for ReplicationQueue.
 */
public class TestReplicationQueue {

  private ReplicationQueue replicationQueue;
  private Random random;

  @Before
  public void setUp() {
    replicationQueue = new ReplicationQueue();
    random = new Random();
  }

  @Test
  public void testDuplicateAddOp() throws InterruptedException {
    long contId = random.nextLong();
    String nodeId = UUID.randomUUID().toString();
    ReplicationRequest obj1, obj2, obj3;
    long time = Time.monotonicNow();
    obj1 = new ReplicationRequest(contId, (short) 2, time, (short) 3);
    obj2 = new ReplicationRequest(contId, (short) 2, time + 1, (short) 3);
    obj3 = new ReplicationRequest(contId, (short) 1, time+2, (short) 3);

    replicationQueue.add(obj1);
    replicationQueue.add(obj2);
    replicationQueue.add(obj3);
    Assert.assertEquals("Should add only 1 msg as second one is duplicate",
        1, replicationQueue.size());
    ReplicationRequest temp = replicationQueue.take();
    Assert.assertEquals(temp, obj3);
  }

  @Test
  public void testPollOp() throws InterruptedException {
    long contId = random.nextLong();
    String nodeId = UUID.randomUUID().toString();
    ReplicationRequest msg1, msg2, msg3, msg4, msg5;
    msg1 = new ReplicationRequest(contId, (short) 1, Time.monotonicNow(),
        (short) 3);
    long time = Time.monotonicNow();
    msg2 = new ReplicationRequest(contId + 1, (short) 4, time, (short) 3);
    msg3 = new ReplicationRequest(contId + 2, (short) 0, time, (short) 3);
    msg4 = new ReplicationRequest(contId, (short) 2, time, (short) 3);
    // Replication message for same container but different nodeId
    msg5 = new ReplicationRequest(contId + 1, (short) 2, time, (short) 3);

    replicationQueue.add(msg1);
    replicationQueue.add(msg2);
    replicationQueue.add(msg3);
    replicationQueue.add(msg4);
    replicationQueue.add(msg5);
    Assert.assertEquals("Should have 3 objects",
        3, replicationQueue.size());

    // Since Priority queue orders messages according to replication count,
    // message with lowest replication should be first
    ReplicationRequest temp;
    temp = replicationQueue.take();
    Assert.assertEquals("Should have 2 objects",
        2, replicationQueue.size());
    Assert.assertEquals(temp, msg3);

    temp = replicationQueue.take();
    Assert.assertEquals("Should have 1 objects",
        1, replicationQueue.size());
    Assert.assertEquals(temp, msg5);

    // Message 2 should be ordered before message 5 as both have same
    // replication number but message 2 has earlier timestamp.
    temp = replicationQueue.take();
    Assert.assertEquals("Should have 0 objects",
        replicationQueue.size(), 0);
    Assert.assertEquals(temp, msg4);
  }

  @Test
  public void testRemoveOp() {
    long contId = random.nextLong();
    String nodeId = UUID.randomUUID().toString();
    ReplicationRequest obj1, obj2, obj3;
    obj1 = new ReplicationRequest(contId, (short) 1, Time.monotonicNow(),
        (short) 3);
    obj2 = new ReplicationRequest(contId + 1, (short) 2, Time.monotonicNow(),
        (short) 3);
    obj3 = new ReplicationRequest(contId + 2, (short) 3, Time.monotonicNow(),
        (short) 3);

    replicationQueue.add(obj1);
    replicationQueue.add(obj2);
    replicationQueue.add(obj3);
    Assert.assertEquals("Should have 3 objects",
        3, replicationQueue.size());

    replicationQueue.remove(obj3);
    Assert.assertEquals("Should have 2 objects",
        2, replicationQueue.size());

    replicationQueue.remove(obj2);
    Assert.assertEquals("Should have 1 objects",
        1, replicationQueue.size());

    replicationQueue.remove(obj1);
    Assert.assertEquals("Should have 0 objects",
        0, replicationQueue.size());
  }

}

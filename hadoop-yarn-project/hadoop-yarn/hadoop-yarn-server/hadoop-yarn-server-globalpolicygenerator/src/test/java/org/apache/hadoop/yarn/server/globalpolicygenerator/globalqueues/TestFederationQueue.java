/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.globalpolicygenerator.globalqueues;

import static org.apache.hadoop.yarn.server.globalpolicygenerator.globalqueues.GlobalQueueTestUtil.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides simple tests for the {@code FederationQueue} data class.
 */
public class TestFederationQueue {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestFederationQueue.class);

  @Test
  public void testParseQueue() throws Exception {
    String queueJson = loadFile("globalqueues/tree-queue.json");
    FederationQueue fq = parseQueue(queueJson);
    fq.validate();
    Assert.assertEquals("root", fq.getQueueName());
    Assert.assertEquals(2, fq.getChildren().size());
    Assert.assertEquals(100000, fq.getGuarCap().getMemorySize());

    FederationQueue queueA = fq.getChildByName("A");
    Assert.assertEquals(2, queueA.getChildren().size());
    Assert.assertEquals(750, queueA.getUsedCap().getVirtualCores());
  }


  @Test
  public void testMergeFedQueue() throws Exception {
    List<FederationQueue> toMerge = generateFedCluster();

    FederationQueue merged =
        FederationQueue.mergeQueues(toMerge,
            SubClusterId.newInstance("merged"));

    merged.propagateCapacities();
    merged.validate();
    LOG.info(merged.toQuickString());
  }

  @Test
  public void testPropagateFedQueue() throws Exception {

    String queueJson = loadFile("globalqueues/tree-queue-adaptable.json");

    int numSubclusters = 10;

    Resource guar = Resource.newInstance(5 * 1024, 10);
    Resource max = Resource.newInstance(8 * 1024, 10);
    Resource used = Resource.newInstance(4 * 1024, 10);
    Resource dem = Resource.newInstance(1 * 1024, 10);

    List<FederationQueue> toMerge = new ArrayList<>();
    for (int i = 0; i < numSubclusters; i++) {
      queueJson = String.format(queueJson, "A1", toJSONString(guar),
          toJSONString(max), toJSONString(used), toJSONString(dem));
      FederationQueue temp = parseQueue(queueJson);
      temp.propagateCapacities();
      temp.validate();
      toMerge.add(temp);
    }

    FederationQueue merged =
        FederationQueue.mergeQueues(toMerge,
            SubClusterId.newInstance("merged"));

    merged.propagateCapacities();
    merged.validate();

    LOG.info(merged.toQuickString());
  }
}

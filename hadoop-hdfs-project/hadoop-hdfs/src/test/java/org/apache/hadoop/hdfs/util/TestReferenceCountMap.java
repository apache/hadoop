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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verify ReferenceCount map in concurrent scenarios.
 */
public class TestReferenceCountMap {
  //Add these number of references in loop
  public static final int LOOP_COUNTER = 10000;
  //Use 2 global features
  private AclFeature aclFeature1 = new AclFeature(new int[]{1});
  private AclFeature aclFeature2 = new AclFeature(new int[]{2});

  @Test
  public void testReferenceCountMap() throws Exception {
    ReferenceCountMap<AclFeature> countMap = new ReferenceCountMap<>();
    countMap.put(aclFeature1);
    countMap.put(aclFeature2);
    Assert.assertEquals(1, countMap.getReferenceCount(aclFeature1));
    Assert.assertEquals(1, countMap.getReferenceCount(aclFeature2));

    countMap.put(aclFeature1);
    countMap.put(aclFeature2);
    Assert.assertEquals(2, countMap.getReferenceCount(aclFeature1));
    Assert.assertEquals(2, countMap.getReferenceCount(aclFeature2));

    countMap.put(aclFeature1);
    Assert.assertEquals(3, countMap.getReferenceCount(aclFeature1));
    countMap.put(aclFeature1);
    Assert.assertEquals(4, countMap.getReferenceCount(aclFeature1));
    Assert.assertEquals(2, countMap.getReferenceCount(aclFeature2));

    //Delete operations:
    countMap.remove(aclFeature1);
    countMap.remove(aclFeature2);
    Assert.assertEquals(3, countMap.getReferenceCount(aclFeature1));
    Assert.assertEquals(1, countMap.getReferenceCount(aclFeature2));

    //Verify unique elements in map
    Assert.assertEquals(2, countMap.getUniqueElementsSize());
  }

  @Test
  public void testRefCountMapConcurrently() throws Exception {
    ReferenceCountMap<AclFeature> countMap = new ReferenceCountMap<>();

    PutThread putThread1 = new PutThread(countMap);
    putThread1.start();
    PutThread putThread2 = new PutThread(countMap);
    putThread2.start();
    RemoveThread removeThread1 = new RemoveThread(countMap);

    putThread1.join();
    putThread2.join();
    Assert.assertEquals(2 * LOOP_COUNTER,
        countMap.getReferenceCount(aclFeature1));
    Assert.assertEquals(2 * LOOP_COUNTER,
        countMap.getReferenceCount(aclFeature2));

    removeThread1.start();
    removeThread1.join();
    Assert.assertEquals(LOOP_COUNTER, countMap.getReferenceCount(aclFeature1));
    Assert.assertEquals(LOOP_COUNTER, countMap.getReferenceCount(aclFeature2));
  }

  class PutThread extends Thread {
    private ReferenceCountMap<AclFeature> referenceCountMap;
    PutThread(ReferenceCountMap<AclFeature> referenceCountMap) {
      this.referenceCountMap = referenceCountMap;
    }
    @Override
    public void run() {
      for (int i = 0; i < LOOP_COUNTER; i++) {
        referenceCountMap.put(aclFeature1);
        referenceCountMap.put(aclFeature2);
      }
    }
  };

  class RemoveThread extends Thread {
    private ReferenceCountMap<AclFeature> referenceCountMap;
    RemoveThread(ReferenceCountMap<AclFeature> referenceCountMap) {
      this.referenceCountMap = referenceCountMap;
    }
    @Override
    public void run() {
      for (int i = 0; i < LOOP_COUNTER; i++) {
        referenceCountMap.remove(aclFeature1);
        referenceCountMap.remove(aclFeature2);
      }
    }
  };
}

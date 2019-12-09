/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.TestPlacementProcessor.schedulingRequest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.BatchedRequests;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Request Iterator.
 */
public class TestBatchedRequestsIterators {

  @Test
  public void testSerialIterator() throws Exception {
    List<SchedulingRequest> schedulingRequestList =
        Arrays.asList(schedulingRequest(1, 1, 1, 512, "foo"),
            schedulingRequest(1, 2, 1, 512, "foo"),
            schedulingRequest(1, 3, 1, 512, "foo"),
            schedulingRequest(1, 4, 1, 512, "foo"));

    BatchedRequests batchedRequests = new BatchedRequests(
        BatchedRequests.IteratorType.SERIAL, null, schedulingRequestList, 1);

    Iterator<SchedulingRequest> requestIterator = batchedRequests.iterator();
    long prevAllocId = 0;
    while (requestIterator.hasNext()) {
      SchedulingRequest request = requestIterator.next();
      Assert.assertTrue(request.getAllocationRequestId() > prevAllocId);
      prevAllocId = request.getAllocationRequestId();
    }
  }

  @Test
  public void testPopularTagsIterator() throws Exception {
    List<SchedulingRequest> schedulingRequestList =
        Arrays.asList(schedulingRequest(1, 1, 1, 512, "pri", "foo"),
            schedulingRequest(1, 2, 1, 512, "bar"),
            schedulingRequest(1, 3, 1, 512, "foo", "pri"),
            schedulingRequest(1, 4, 1, 512, "test"),
            schedulingRequest(1, 5, 1, 512, "pri", "bar"));

    BatchedRequests batchedRequests =
        new BatchedRequests(BatchedRequests.IteratorType.POPULAR_TAGS, null,
            schedulingRequestList, 1);

    Iterator<SchedulingRequest> requestIterator = batchedRequests.iterator();
    long recCcount = 0;
    while (requestIterator.hasNext()) {
      SchedulingRequest request = requestIterator.next();
      if (recCcount < 3) {
        Assert.assertTrue(request.getAllocationTags().contains("pri"));
      } else {
        Assert.assertTrue(request.getAllocationTags().contains("bar")
            || request.getAllocationTags().contains("test"));
      }
      recCcount++;
    }
  }
}
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

package org.apache.hadoop.tools.util;

import org.apache.hadoop.tools.util.ProducerConsumer;
import org.apache.hadoop.tools.util.WorkReport;
import org.apache.hadoop.tools.util.WorkRequest;
import org.apache.hadoop.tools.util.WorkRequestProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.lang.Exception;
import java.lang.Integer;

public class TestProducerConsumer {
  public class CopyProcessor implements WorkRequestProcessor<Integer, Integer> {
    public WorkReport<Integer> processItem(WorkRequest<Integer> workRequest) {
      Integer item = new Integer(workRequest.getItem());
      return new WorkReport<Integer>(item, 0, true);
    }
  }

  public class ExceptionProcessor implements WorkRequestProcessor<Integer, Integer> {
    @SuppressWarnings("null")
    public WorkReport<Integer> processItem(WorkRequest<Integer> workRequest) {
      try {
        Integer item = null;
        item.intValue(); // Throw NULL pointer exception.

        // We should never be here (null pointer exception above)
        return new WorkReport<Integer>(item, 0, true);
      } catch (Exception e) {
        Integer item = new Integer(workRequest.getItem());
        return new WorkReport<Integer>(item, 1, false, e);
      }
    }
  }

  @Test
  public void testSimpleProducerConsumer() {
    ProducerConsumer<Integer, Integer> worker =
        new ProducerConsumer<Integer, Integer>(1);
    worker.addWorker(new CopyProcessor());
    worker.put(new WorkRequest<Integer>(42));
    try {
      WorkReport<Integer> report = worker.take();
      Assert.assertEquals(42, report.getItem().intValue());
    } catch (InterruptedException ie) {
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testMultipleProducerConsumer() {
    ProducerConsumer<Integer, Integer> workers =
        new ProducerConsumer<Integer, Integer>(10);
    for (int i = 0; i < 10; i++) {
      workers.addWorker(new CopyProcessor());
    }

    int sum = 0;
    int numRequests = 2000;
    for (int i = 0; i < numRequests; i++) {
      workers.put(new WorkRequest<Integer>(i + 42));
      sum += i + 42;
    }

    int numReports = 0;
    while (workers.getWorkCnt() > 0) {
      WorkReport<Integer> report = workers.blockingTake();
      sum -= report.getItem().intValue();
      numReports++;
    }
    Assert.assertEquals(0, sum);
    Assert.assertEquals(numRequests, numReports);
  }

  @Test
  public void testExceptionProducerConsumer() {
    ProducerConsumer<Integer, Integer> worker =
        new ProducerConsumer<Integer, Integer>(1);
    worker.addWorker(new ExceptionProcessor());
    worker.put(new WorkRequest<Integer>(42));
    try {
      WorkReport<Integer> report = worker.take();
      Assert.assertEquals(42, report.getItem().intValue());
      Assert.assertFalse(report.getSuccess());
      Assert.assertNotNull(report.getException());
    } catch (InterruptedException ie) {
      Assert.assertTrue(false);
    }
  }
}

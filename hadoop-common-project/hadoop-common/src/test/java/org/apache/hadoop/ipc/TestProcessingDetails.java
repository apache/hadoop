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

package org.apache.hadoop.ipc;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ipc.ProcessingDetails.Timing;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for ProcessingDetails time unit conversion and output.
 */
public class TestProcessingDetails {

  /**
   * Test that the conversion of time values in various units in and out of the
   * details are done properly.
   */
  @Test
  public void testTimeConversion() {
    ProcessingDetails details = new ProcessingDetails(TimeUnit.MICROSECONDS);

    details.set(Timing.ENQUEUE, 10);
    assertEquals(10, details.get(Timing.ENQUEUE));
    assertEquals(10_000, details.get(Timing.ENQUEUE, TimeUnit.NANOSECONDS));

    details.set(Timing.QUEUE, 20, TimeUnit.MILLISECONDS);
    details.add(Timing.QUEUE, 20, TimeUnit.MICROSECONDS);
    assertEquals(20_020, details.get(Timing.QUEUE));
    assertEquals(0, details.get(Timing.QUEUE, TimeUnit.SECONDS));
  }

  @Test
  public void testToString() {
    ProcessingDetails details = new ProcessingDetails(TimeUnit.MICROSECONDS);
    details.set(Timing.ENQUEUE, 10);
    details.set(Timing.QUEUE, 20, TimeUnit.MILLISECONDS);

    assertEquals("enqueueTime=10 queueTime=20000 handlerTime=0 " +
        "processingTime=0 lockfreeTime=0 lockwaitTime=0 locksharedTime=0 " +
        "lockexclusiveTime=0 responseTime=0", details.toString());
  }
}

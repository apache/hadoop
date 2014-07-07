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

package org.apache.hadoop.yarn.util;

import org.junit.Assert;
import org.junit.Test;

public class TestTimes {

  @Test
  public void testNegativeStartTimes() {
    long elapsed = Times.elapsed(-5, 10, true);
    Assert.assertEquals("Elapsed time is not 0", 0, elapsed);
    elapsed = Times.elapsed(-5, 10, false);
    Assert.assertEquals("Elapsed time is not -1", -1, elapsed);
  }

  @Test
  public void testNegativeFinishTimes() {
    long elapsed = Times.elapsed(5, -10, false);
    Assert.assertEquals("Elapsed time is not -1", -1, elapsed);
  }

  @Test
  public void testNegativeStartandFinishTimes() {
    long elapsed = Times.elapsed(-5, -10, false);
    Assert.assertEquals("Elapsed time is not -1", -1, elapsed);
  }

  @Test
  public void testPositiveStartandFinishTimes() {
    long elapsed = Times.elapsed(5, 10, true);
    Assert.assertEquals("Elapsed time is not 5", 5, elapsed);
    elapsed = Times.elapsed(5, 10, false);
    Assert.assertEquals("Elapsed time is not 5", 5, elapsed);
  }

  @Test
  public void testFinishTimesAheadOfStartTimes() {
    long elapsed = Times.elapsed(10, 5, true);
    Assert.assertEquals("Elapsed time is not -1", -1, elapsed);
    elapsed = Times.elapsed(10, 5, false);
    Assert.assertEquals("Elapsed time is not -1", -1, elapsed);
    // use Long.MAX_VALUE to ensure started time is after the current one
    elapsed = Times.elapsed(Long.MAX_VALUE, 0, true);
    Assert.assertEquals("Elapsed time is not -1", -1, elapsed);
  }
}
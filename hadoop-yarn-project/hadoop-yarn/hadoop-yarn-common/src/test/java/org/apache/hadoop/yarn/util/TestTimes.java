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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.jupiter.api.Test;

import static org.apache.hadoop.yarn.util.Times.ISO8601_DATE_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTimes {

  @Test
  void testNegativeStartTimes() {
    long elapsed = Times.elapsed(-5, 10, true);
    assertEquals(0, elapsed, "Elapsed time is not 0");
    elapsed = Times.elapsed(-5, 10, false);
    assertEquals(-1, elapsed, "Elapsed time is not -1");
  }

  @Test
  void testNegativeFinishTimes() {
    long elapsed = Times.elapsed(5, -10, false);
    assertEquals(-1, elapsed, "Elapsed time is not -1");
  }

  @Test
  void testNegativeStartandFinishTimes() {
    long elapsed = Times.elapsed(-5, -10, false);
    assertEquals(-1, elapsed, "Elapsed time is not -1");
  }

  @Test
  void testPositiveStartandFinishTimes() {
    long elapsed = Times.elapsed(5, 10, true);
    assertEquals(5, elapsed, "Elapsed time is not 5");
    elapsed = Times.elapsed(5, 10, false);
    assertEquals(5, elapsed, "Elapsed time is not 5");
  }

  @Test
  void testFinishTimesAheadOfStartTimes() {
    long elapsed = Times.elapsed(10, 5, true);
    assertEquals(-1, elapsed, "Elapsed time is not -1");
    elapsed = Times.elapsed(10, 5, false);
    assertEquals(-1, elapsed, "Elapsed time is not -1");
    // use Long.MAX_VALUE to ensure started time is after the current one
    elapsed = Times.elapsed(Long.MAX_VALUE, 0, true);
    assertEquals(-1, elapsed, "Elapsed time is not -1");
  }

  @Test
  void validateISO() throws IOException {
    SimpleDateFormat isoFormat = new SimpleDateFormat(ISO8601_DATE_FORMAT);
    for (int i = 0; i < 1000; i++) {
      long now = System.currentTimeMillis();
      String instant =  Times.formatISO8601(now);
      String date = isoFormat.format(new Date(now));
      assertEquals(date, instant);
    }
  }
}
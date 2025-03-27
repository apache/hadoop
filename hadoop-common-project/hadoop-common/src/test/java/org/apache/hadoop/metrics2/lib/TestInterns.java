/*
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

package org.apache.hadoop.metrics2.lib;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import static org.apache.hadoop.metrics2.lib.Interns.*;

public class TestInterns {

  @Test public void testInfo() {
    MetricsInfo info = info("m", "m desc");
    assertSame(info, info("m", "m desc"), "same info");
  }

  @Test public void testTag() {
    MetricsTag tag = tag("t", "t desc", "t value");
    assertSame(tag, tag("t", "t desc", "t value"), "same tag");
  }

  @Test public void testInfoOverflow() {
    MetricsInfo i0 = info("m0", "m desc");
    for (int i = 0; i < MAX_INFO_NAMES + 1; ++i) {
      info("m"+ i, "m desc");
      if (i < MAX_INFO_NAMES) {
        assertSame(i0, info("m0", "m desc"), "m0 is still there");
      }
    }
    assertNotSame(i0, info("m0", "m desc"), "m0 is gone");

    MetricsInfo i1 = info("m1", "m desc");
    for (int i = 0; i < MAX_INFO_DESCS; ++i) {
      info("m1", "m desc"+ i);
      if (i < MAX_INFO_DESCS - 1) {
        assertSame(i1, info("m1", "m desc"), "i1 is still there");
      }
    }
    assertNotSame(i1,  info("m1", "m desc"), "i1 is gone");
  }

  @Test public void testTagOverflow() {
    MetricsTag t0 = tag("t0", "t desc", "t value");
    for (int i = 0; i < MAX_TAG_NAMES + 1; ++i) {
      tag("t"+ i, "t desc", "t value");
      if (i < MAX_TAG_NAMES) {
        assertSame(t0, tag("t0", "t desc", "t value"), "t0 still there");
      }
    }
    assertNotSame(t0, tag("t0", "t desc", "t value"), "t0 is gone");

    MetricsTag t1 = tag("t1", "t desc", "t value");
    for (int i = 0; i < MAX_TAG_VALUES; ++i) {
      tag("t1", "t desc", "t value"+ i);
      if (i < MAX_TAG_VALUES -1) {
        assertSame(t1, tag("t1", "t desc", "t value"), "t1 is still there");
      }
    }
    assertNotSame(t1, tag("t1", "t desc", "t value"), "t1 is gone");
  }
}

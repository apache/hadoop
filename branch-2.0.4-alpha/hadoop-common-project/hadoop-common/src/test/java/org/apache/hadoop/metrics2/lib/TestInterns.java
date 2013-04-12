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

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import static org.apache.hadoop.metrics2.lib.Interns.*;

public class TestInterns {

  @Test public void testInfo() {
    MetricsInfo info = info("m", "m desc");
    assertSame("same info", info, info("m", "m desc"));
  }

  @Test public void testTag() {
    MetricsTag tag = tag("t", "t desc", "t value");
    assertSame("same tag", tag, tag("t", "t desc", "t value"));
  }

  @Test public void testInfoOverflow() {
    MetricsInfo i0 = info("m0", "m desc");
    for (int i = 0; i < MAX_INFO_NAMES + 1; ++i) {
      info("m"+ i, "m desc");
      if (i < MAX_INFO_NAMES) {
        assertSame("m0 is still there", i0, info("m0", "m desc"));
      }
    }
    assertNotSame("m0 is gone", i0, info("m0", "m desc"));

    MetricsInfo i1 = info("m1", "m desc");
    for (int i = 0; i < MAX_INFO_DESCS; ++i) {
      info("m1", "m desc"+ i);
      if (i < MAX_INFO_DESCS - 1) {
        assertSame("i1 is still there", i1, info("m1", "m desc"));
      }
    }
    assertNotSame("i1 is gone", i1,  info("m1", "m desc"));
  }

  @Test public void testTagOverflow() {
    MetricsTag t0 = tag("t0", "t desc", "t value");
    for (int i = 0; i < MAX_TAG_NAMES + 1; ++i) {
      tag("t"+ i, "t desc", "t value");
      if (i < MAX_TAG_NAMES) {
        assertSame("t0 still there", t0, tag("t0", "t desc", "t value"));
      }
    }
    assertNotSame("t0 is gone", t0, tag("t0", "t desc", "t value"));

    MetricsTag t1 = tag("t1", "t desc", "t value");
    for (int i = 0; i < MAX_TAG_VALUES; ++i) {
      tag("t1", "t desc", "t value"+ i);
      if (i < MAX_TAG_VALUES -1) {
        assertSame("t1 is still there", t1, tag("t1", "t desc", "t value"));
      }
    }
    assertNotSame("t1 is gone", t1, tag("t1", "t desc", "t value"));
  }
}

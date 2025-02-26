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
package org.apache.hadoop.util;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The class to test DurationInfo.
 */
public class TestDurationInfo {
  private final Logger log = LoggerFactory.getLogger(TestDurationInfo.class);

  @Test
  public void testDurationInfoCreation() throws Exception {
    DurationInfo info = new DurationInfo(log, "test");
    assertTrue(info.value() >= 0);
    Thread.sleep(1000);
    info.finished();
    assertTrue(info.value() > 0);

    info = new DurationInfo(log, true, "test format %s", "value");
    assertEquals("test format value: duration 0:00.000s",
        info.toString());

    info = new DurationInfo(log, false, "test format %s", "value");
    assertEquals("test format value: duration 0:00.000s",
        info.toString());
  }

  @Test
  public void testDurationInfoWithMultipleClose() throws Exception {
    DurationInfo info = new DurationInfo(log, "test");
    Thread.sleep(1000);
    info.close();
    info.close();
    assertTrue(info.value() > 0);
  }

  @Test
  public void testDurationInfoCreationWithNullMsg() {
    assertThrows(NullPointerException.class, ()->{
      DurationInfo info = new DurationInfo(log, null);
      info.close();
    });
  }
}

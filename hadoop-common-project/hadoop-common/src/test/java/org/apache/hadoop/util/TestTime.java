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

import static java.time.ZoneId.systemDefault;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

/**
 * A JUnit test to test {@link Time}.
 */
public class TestTime {

  /**
   * Test formatTime.
   */
  @Test
  public void testFormatTime() {
    ZonedDateTime time = LocalDateTime.of(1999, 12, 31,
            23, 59, 59, 999000000).atZone(systemDefault());
    long timeMillis = time.toInstant().toEpochMilli();
    String zoneSuffix = DateTimeFormatter.ofPattern("Z").format(time);
    String expected = "1999-12-31 23:59:59,999" + zoneSuffix;
    assertEquals(expected, Time.formatTime(timeMillis));
  }
}

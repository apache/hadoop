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

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;

import org.junit.Test;

/**
 * A JUnit test to test {@link Time}.
 */
public class TestTime {

  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSSZ");
    }
  };

  /**
   * Test formatTime.
   * @throws IOException
   */
  @Test
  public void testFormatTime() {
    long time = Time.now();
    assertEquals(Time.formatTime(time),
        DATE_FORMAT.get().format(time));
  }
}

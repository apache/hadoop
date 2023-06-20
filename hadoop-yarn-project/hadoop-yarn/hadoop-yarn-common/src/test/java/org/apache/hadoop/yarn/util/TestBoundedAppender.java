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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link BoundedAppender}.
 */
public class TestBoundedAppender {

  @Test
  void initWithZeroLimitThrowsException() {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {

      new BoundedAppender(0);
    });
    assertTrue(exception.getMessage().contains("limit should be positive"));
  }

  @Test
  void nullAppendedNullStringRead() {
    final BoundedAppender boundedAppender = new BoundedAppender(4);
    boundedAppender.append(null);

    assertEquals("null",
        boundedAppender.toString(),
        "null appended, \"null\" read");
  }

  @Test
  void appendBelowLimitOnceValueIsReadCorrectly() {
    final BoundedAppender boundedAppender = new BoundedAppender(2);

    boundedAppender.append("ab");

    assertEquals("ab",
        boundedAppender.toString(),
        "value appended is read correctly");
  }

  @Test
  void appendValuesBelowLimitAreReadCorrectlyInFifoOrder() {
    final BoundedAppender boundedAppender = new BoundedAppender(3);

    boundedAppender.append("ab");
    boundedAppender.append("cd");
    boundedAppender.append("e");
    boundedAppender.append("fg");

    assertEquals(String.format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 7, "efg"),
        boundedAppender.toString(),
        "last values appended fitting limit are read correctly");
  }

  @Test
  void appendLastAboveLimitPreservesLastMessagePostfix() {
    final BoundedAppender boundedAppender = new BoundedAppender(3);

    boundedAppender.append("ab");
    boundedAppender.append("cde");
    boundedAppender.append("fghij");

    assertEquals(
        String
            .format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 10, "hij"),
        boundedAppender.toString(),
        "last value appended above limit postfix is read correctly");
  }

  @Test
  void appendMiddleAboveLimitPreservesLastMessageAndMiddlePostfix() {
    final BoundedAppender boundedAppender = new BoundedAppender(3);

    boundedAppender.append("ab");
    boundedAppender.append("cde");

    assertEquals(String.format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 5, "cde"),
        boundedAppender.toString(),
        "last value appended above limit postfix is read correctly");

    boundedAppender.append("fg");

    assertEquals(
        String.format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 7, "efg"),
        boundedAppender.toString(),
        "middle value appended above limit postfix and last value are "
            + "read correctly");

    boundedAppender.append("hijkl");

    assertEquals(
        String
            .format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 12, "jkl"),
        boundedAppender.toString(),
        "last value appended above limit postfix is read correctly");
  }
}
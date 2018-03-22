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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link BoundedAppender}.
 */
public class TestBoundedAppender {
  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void initWithZeroLimitThrowsException() {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("limit should be positive");

    new BoundedAppender(0);
  }

  @Test
  public void nullAppendedNullStringRead() {
    final BoundedAppender boundedAppender = new BoundedAppender(4);
    boundedAppender.append(null);

    assertEquals("null appended, \"null\" read", "null",
        boundedAppender.toString());
  }

  @Test
  public void appendBelowLimitOnceValueIsReadCorrectly() {
    final BoundedAppender boundedAppender = new BoundedAppender(2);

    boundedAppender.append("ab");

    assertEquals("value appended is read correctly", "ab",
        boundedAppender.toString());
  }

  @Test
  public void appendValuesBelowLimitAreReadCorrectlyInFifoOrder() {
    final BoundedAppender boundedAppender = new BoundedAppender(3);

    boundedAppender.append("ab");
    boundedAppender.append("cd");
    boundedAppender.append("e");
    boundedAppender.append("fg");

    assertEquals("last values appended fitting limit are read correctly",
        String.format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 7, "efg"),
        boundedAppender.toString());
  }

  @Test
  public void appendLastAboveLimitPreservesLastMessagePostfix() {
    final BoundedAppender boundedAppender = new BoundedAppender(3);

    boundedAppender.append("ab");
    boundedAppender.append("cde");
    boundedAppender.append("fghij");

    assertEquals(
        "last value appended above limit postfix is read correctly", String
            .format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 10, "hij"),
        boundedAppender.toString());
  }

  @Test
  public void appendMiddleAboveLimitPreservesLastMessageAndMiddlePostfix() {
    final BoundedAppender boundedAppender = new BoundedAppender(3);

    boundedAppender.append("ab");
    boundedAppender.append("cde");

    assertEquals("last value appended above limit postfix is read correctly",
        String.format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 5, "cde"),
        boundedAppender.toString());

    boundedAppender.append("fg");

    assertEquals(
        "middle value appended above limit postfix and last value are "
            + "read correctly",
        String.format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 7, "efg"),
        boundedAppender.toString());

    boundedAppender.append("hijkl");

    assertEquals(
        "last value appended above limit postfix is read correctly", String
            .format(BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 3, 12, "jkl"),
        boundedAppender.toString());
  }
}
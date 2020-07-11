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

package org.apache.hadoop.util.noguava;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPreconditions {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestPreconditions.class);

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();


  @Test
  public void testCheckIsTrueFailure() {
    exceptionRule.expect(IllegalArgumentException.class);
    Preconditions.checkIsTrue(false);
  }

  @Test
  public void testCheckIsTrueMessageFailure() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("testCheckIsTrueMessageFailure");
    Preconditions.checkIsTrue(false,
        "testCheckIsTrueMessageFailure");
  }

  @Test
  public void testCheckIsTrueMessageObjArgsFailure() {
    final String message = "MessageObjArgsFailure %s, %d, %f";
    final String formatted = String.format(message, "Hello",
        new Integer(1), new Double(2));
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage(formatted);
    Preconditions.checkIsTrue(false, message,
        "Hello",
        new Integer(1),
        new Double(2));
  }
}

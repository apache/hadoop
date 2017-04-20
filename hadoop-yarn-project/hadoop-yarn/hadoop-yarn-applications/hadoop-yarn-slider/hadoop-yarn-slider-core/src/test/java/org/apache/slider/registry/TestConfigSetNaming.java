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

package org.apache.slider.registry;

import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test config set name validation.
 */
public class TestConfigSetNaming {

  void assertValid(String name) {
    PublishedConfigSet.validateName(name);
  }

  void assertInvalid(String name) {
    try {
      PublishedConfigSet.validateName(name);
      Assert.fail("Invalid name was unexpectedly parsed: " + name);
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testLowerCase() throws Throwable {
    assertValid("abcdefghijklmnopqrstuvwxyz");
  }

  @Test
  public void testUpperCaseInvalid() throws Throwable {
    assertInvalid("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
  }

  @Test
  public void testNumbers() throws Throwable {
    assertValid("01234567890");
  }

  @Test
  public void testChars() throws Throwable {
    assertValid("a-_+");
  }

  @Test
  public void testInvalids() throws Throwable {
    for (String s : Arrays.asList(
        "",
        " ",
        "*",
        "a/b",
        "b\\a",
        "\"",
        "'",
        "\u0000",
        "\u0f00",
        "key.value",
        "-",
        "+",
        "_",
        "?")) {
      assertInvalid(s);
    }
  }
}

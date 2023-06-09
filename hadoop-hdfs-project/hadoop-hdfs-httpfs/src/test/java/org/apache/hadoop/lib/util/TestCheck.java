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

package org.apache.hadoop.lib.util;


import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.test.HTestCase;
import org.junit.Test;

public class TestCheck extends HTestCase {

  @Test
  public void notNullNotNull() {
    assertEquals(Check.notNull("value", "name"), "value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void notNullNull() {
    Check.notNull(null, "name");
  }

  @Test
  public void notNullElementsNotNull() {
    Check.notNullElements(new ArrayList<String>(), "name");
    Check.notNullElements(Arrays.asList("a"), "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void notNullElementsNullList() {
    Check.notNullElements(null, "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void notNullElementsNullElements() {
    Check.notNullElements(Arrays.asList("a", "", null), "name");
  }

  @Test
  public void notEmptyElementsNotNull() {
    Check.notEmptyElements(new ArrayList<String>(), "name");
    Check.notEmptyElements(Arrays.asList("a"), "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void notEmptyElementsNullList() {
    Check.notEmptyElements(null, "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void notEmptyElementsNullElements() {
    Check.notEmptyElements(Arrays.asList("a", null), "name");
  }


  @Test(expected = IllegalArgumentException.class)
  public void notEmptyElementsEmptyElements() {
    Check.notEmptyElements(Arrays.asList("a", ""), "name");
  }


  @Test
  public void notEmptyNotEmtpy() {
    assertEquals(Check.notEmpty("value", "name"), "value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void notEmptyNull() {
    Check.notEmpty(null, "name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void notEmptyEmpty() {
    Check.notEmpty("", "name");
  }

  @Test
  public void validIdentifierValid() throws Exception {
    assertEquals(Check.validIdentifier("a", 1, ""), "a");
    assertEquals(Check.validIdentifier("a1", 2, ""), "a1");
    assertEquals(Check.validIdentifier("a_", 3, ""), "a_");
    assertEquals(Check.validIdentifier("_", 1, ""), "_");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validIdentifierInvalid1() throws Exception {
    Check.validIdentifier("!", 1, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validIdentifierInvalid2() throws Exception {
    Check.validIdentifier("a1", 1, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validIdentifierInvalid3() throws Exception {
    Check.validIdentifier("1", 1, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validIdentifierInvalid4() throws Exception {
    Check.validIdentifier("`a", 2, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void validIdentifierInvalid5() throws Exception {
    Check.validIdentifier("[a", 2, "");
  }

  @Test
  public void checkGTZeroGreater() {
    assertEquals(Check.gt0(120, "test"), 120);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkGTZeroZero() {
    Check.gt0(0, "test");
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkGTZeroLessThanZero() {
    Check.gt0(-1, "test");
  }

  @Test
  public void checkGEZero() {
    assertEquals(Check.ge0(120, "test"), 120);
    assertEquals(Check.ge0(0, "test"), 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkGELessThanZero() {
    Check.ge0(-1, "test");
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.service.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test cluster name validation.
 */
public class TestValidateServiceNames {

  void assertValidName(String name) {
    ServiceApiUtil.validateNameFormat(name, new Configuration());
  }

  void assertInvalidName(String name) {
    try {
      ServiceApiUtil.validateNameFormat(name, new Configuration());
      Assert.fail();
    } catch (IllegalArgumentException e) {
      //
    }
  }

  void assertInvalid(List<String> names) {
    for (String name : names) {
      assertInvalidName(name);
    }
  }

  void assertValid(List<String> names) {
    for (String name : names) {
      assertValidName(name);
    }
  }

  @Test
  public void testEmptyName() throws Throwable {
    assertInvalidName("");
  }

  @Test
  public void testSpaceName() throws Throwable {
    assertInvalidName(" ");
  }


  @Test
  public void testLeadingHyphen() throws Throwable {
    assertInvalidName("-hyphen");
  }

  @Test
  public void testTitleLetters() throws Throwable {
    assertInvalidName("Title");
  }

  @Test
  public void testCapitalLetters() throws Throwable {
    assertInvalidName("UPPER-CASE-CLUSTER");
  }

  @Test
  public void testInnerBraced() throws Throwable {
    assertInvalidName("a[a");
  }

  @Test
  public void testLeadingBrace() throws Throwable {
    assertInvalidName("[");
  }

  @Test
  public void testNonalphaLeadingChars() throws Throwable {
    assertInvalid(Arrays.asList(
        "[a", "#", "@", "=", "*", "."
    ));
  }

  @Test
  public void testNonalphaInnerChars() throws Throwable {
    assertInvalid(Arrays.asList(
        "a[a", "b#", "c@", "d=", "e*", "f.", "g ", "h i"
    ));
  }

  @Test
  public void testClusterValid() throws Throwable {
    assertValidName("cluster");
  }

  @Test
  public void testValidNames() throws Throwable {
    assertValid(Arrays.asList(
        "cluster",
        "cluster1",
        "very-very-very-long-cluster-name",
        "c1234567890"
    ));

  }

}

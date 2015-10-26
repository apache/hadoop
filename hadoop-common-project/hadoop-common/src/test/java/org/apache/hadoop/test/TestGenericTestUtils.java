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

package org.apache.hadoop.test;

import org.junit.Test;

public class TestGenericTestUtils extends GenericTestUtils {

  @Test
  public void testAssertExceptionContainsNullEx() throws Throwable {
    try {
      assertExceptionContains("", null);
    } catch (AssertionError e) {
      if (!e.toString().contains(E_NULL_THROWABLE)) {
        throw e;
      }
    }
  }

  @Test
  public void testAssertExceptionContainsNullString() throws Throwable {
    try {
      assertExceptionContains("", new BrokenException());
    } catch (AssertionError e) {
      if (!e.toString().contains(E_NULL_THROWABLE_STRING)) {
        throw e;
      }
    }
  }

  @Test
  public void testAssertExceptionContainsWrongText() throws Throwable {
    try {
      assertExceptionContains("Expected", new Exception("(actual)"));
    } catch (AssertionError e) {
      String s = e.toString();
      if (!s.contains(E_UNEXPECTED_EXCEPTION)
          || !s.contains("(actual)") ) {
        throw e;
      }
      if (e.getCause() == null) {
        throw new AssertionError("No nested cause in assertion", e);
      }
    }
  }

  @Test
  public void testAssertExceptionContainsWorking() throws Throwable {
    assertExceptionContains("Expected", new Exception("Expected"));
  }

  private static class BrokenException extends Exception {
    public BrokenException() {
    }

    @Override
    public String toString() {
      return null;
    }
  }

}

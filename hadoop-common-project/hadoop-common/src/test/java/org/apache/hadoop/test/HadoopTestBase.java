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

import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * A base class for JUnit4 tests that sets a default timeout for all tests
 * that subclass this test
 */
public abstract class HadoopTestBase {
  /**
   * System property name to set the test timeout: {@value}
   */
  public static final String PROPERTY_TEST_DEFAULT_TIMEOUT =
    "test.default.timeout";

  /**
   * The default timeout (in milliseconds) if the system property
   * {@link #PROPERTY_TEST_DEFAULT_TIMEOUT}
   * is not set: {@value}
   */
  public static final int TEST_DEFAULT_TIMEOUT_VALUE = 100000;

  /**
   * The JUnit rule that sets the default timeout for tests
   */
  @Rule
  public Timeout defaultTimeout = retrieveTestTimeout();

  /**
   * Retrieve the test timeout from the system property
   * {@link #PROPERTY_TEST_DEFAULT_TIMEOUT}, falling back to
   * the value in {@link #TEST_DEFAULT_TIMEOUT_VALUE} if the
   * property is not defined.
   * @return the recommended timeout for tests
   */
  public static Timeout retrieveTestTimeout() {
    String propval = System.getProperty(PROPERTY_TEST_DEFAULT_TIMEOUT,
                                         Integer.toString(
                                           TEST_DEFAULT_TIMEOUT_VALUE));
    int millis;
    try {
      millis = Integer.parseInt(propval);
    } catch (NumberFormatException e) {
      //fall back to the default value, as the property cannot be parsed
      millis = TEST_DEFAULT_TIMEOUT_VALUE;
    }
    return new Timeout(millis);
  }
}

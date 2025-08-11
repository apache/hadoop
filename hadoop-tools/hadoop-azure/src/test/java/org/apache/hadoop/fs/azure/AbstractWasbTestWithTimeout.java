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

package org.apache.hadoop.fs.azure;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.apache.hadoop.test.TestName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Base class for any Wasb test with timeouts & named threads.
 * This class does not attempt to bind to Azure.
 */
@Timeout(value = AzureTestConstants.AZURE_TEST_TIMEOUT, unit = TimeUnit.MILLISECONDS)
public class AbstractWasbTestWithTimeout extends Assertions {

  /**
   * The name of the current method.
   */
  @RegisterExtension
  public TestName methodName = new TestName();

  /**
   * Name the junit thread for the class. This will overridden
   * before the individual test methods are run.
   */
  @BeforeAll
  public static void nameTestThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Name the thread to the current test method.
   */
  @BeforeEach
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  /**
   * Override point: the test timeout in milliseconds.
   * @return a timeout in milliseconds
   */
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.AZURE_TEST_TIMEOUT;
  }

  public static void assumeNotNull(Object objects) {
    assumeThat(objects).isNotNull();
  }

  public static void assumeNotNull(Object objects, String message) {
    assumeThat(objects).as(message).isNotNull();
  }
}

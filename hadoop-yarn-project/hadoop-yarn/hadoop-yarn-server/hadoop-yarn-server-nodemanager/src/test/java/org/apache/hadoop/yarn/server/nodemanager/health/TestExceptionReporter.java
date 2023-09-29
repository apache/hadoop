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
package org.apache.hadoop.yarn.server.nodemanager.health;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link ExceptionReporter} class.
 */
public class TestExceptionReporter {
  @Test
  public void testUnhealthy() {
    ExceptionReporter reporter = new ExceptionReporter();
    assertThat(reporter.isHealthy()).isTrue();
    assertThat(reporter.getLastHealthReportTime()).isZero();

    String message = "test";
    Exception exception = new Exception(message);
    reporter.reportException(exception);
    assertThat(reporter.isHealthy()).isFalse();
    assertThat(reporter.getHealthReport()).isEqualTo(message);
    assertThat(reporter.getLastHealthReportTime()).isNotEqualTo(0);
  }
}

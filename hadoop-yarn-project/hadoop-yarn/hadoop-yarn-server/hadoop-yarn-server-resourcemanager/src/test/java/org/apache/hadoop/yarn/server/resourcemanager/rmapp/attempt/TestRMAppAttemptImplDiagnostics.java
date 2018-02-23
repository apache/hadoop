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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.util.BoundedAppender;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Testing {@link RMAppAttemptImpl#diagnostics} scenarios.
 */
public class TestRMAppAttemptImplDiagnostics {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void whenCreatedWithDefaultConfigurationSuccess() {
    final Configuration configuration = new Configuration();
    configuration.setInt(YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC,
        YarnConfiguration.DEFAULT_APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC);

    createRMAppAttemptImpl(configuration);
  }

  @Test
  public void whenCreatedWithWrongConfigurationError() {
    final Configuration configuration = new Configuration();
    configuration.setInt(YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC, 0);

    expectedException.expect(YarnRuntimeException.class);

    createRMAppAttemptImpl(configuration);
  }

  @Test
  public void whenAppendedWithinLimitMessagesArePreserved() {
    final Configuration configuration = new Configuration();
    configuration.setInt(YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC, 1);

    final RMAppAttemptImpl appAttempt = createRMAppAttemptImpl(configuration);

    final String withinLimit = RandomStringUtils.random(1024);
    appAttempt.appendDiagnostics(withinLimit);

    assertEquals("messages within limit should be preserved", withinLimit,
        appAttempt.getDiagnostics());
  }

  @Test
  public void whenAppendedBeyondLimitMessagesAreTruncated() {
    final Configuration configuration = new Configuration();
    configuration.setInt(YarnConfiguration.APP_ATTEMPT_DIAGNOSTICS_LIMIT_KC, 1);

    final RMAppAttemptImpl appAttempt = createRMAppAttemptImpl(configuration);

    final String beyondLimit = RandomStringUtils.random(1025);
    appAttempt.appendDiagnostics(beyondLimit);

    final String truncated = String.format(
        BoundedAppender.TRUNCATED_MESSAGES_TEMPLATE, 1024,
        1025, beyondLimit.substring(1));

    assertEquals("messages beyond limit should be truncated", truncated,
        appAttempt.getDiagnostics());
  }

  private RMAppAttemptImpl createRMAppAttemptImpl(
      final Configuration configuration) {
    final ApplicationAttemptId mockApplicationAttemptId =
        mock(ApplicationAttemptId.class);
    final ApplicationId mockApplicationId = mock(ApplicationId.class);
    when(mockApplicationAttemptId.getApplicationId())
        .thenReturn(mockApplicationId);

    final RMContext mockRMContext = mock(RMContext.class);
    final Dispatcher mockDispatcher = mock(Dispatcher.class);
    when(mockRMContext.getDispatcher()).thenReturn(mockDispatcher);

    return new RMAppAttemptImpl(mockApplicationAttemptId, mockRMContext, null,
        null, null, configuration, null, null);
  }
}
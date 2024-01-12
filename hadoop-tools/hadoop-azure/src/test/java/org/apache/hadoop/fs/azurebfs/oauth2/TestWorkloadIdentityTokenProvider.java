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

package org.apache.hadoop.fs.azurebfs.oauth2;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the refresh logic of workload identity tokens.
 */
public class TestWorkloadIdentityTokenProvider extends AbstractAbfsTestWithTimeout {

  private static final String AUTHORITY = "authority";
  private static final String TENANT_ID = "00000000-0000-0000-0000-000000000000";
  private static final String CLIENT_ID = "00000000-0000-0000-0000-000000000000";
  private static final String TOKEN_FILE = "/tmp/does_not_exist";

  public TestWorkloadIdentityTokenProvider() throws Exception {
  }

  /**
   * Test that the token starts as expired.
   */
  @Test
  public void testTokenStartsAsExpired() {
    WorkloadIdentityTokenProvider provider = new WorkloadIdentityTokenProvider(
        AUTHORITY, TENANT_ID, CLIENT_ID, TOKEN_FILE);

    assertTrue(provider.hasEnoughTimeElapsedSinceLastRefresh());
  }

  /**
   * Test that the token will expire one hour after the last refresh.
   */
  @Test
  public void testTokenExpiresAfterOneHour() {
    WorkloadIdentityTokenProvider provider = Mockito.spy(
        new WorkloadIdentityTokenProvider(AUTHORITY, TENANT_ID, CLIENT_ID, TOKEN_FILE));

    Mockito.doCallRealMethod().when(provider).hasEnoughTimeElapsedSinceLastRefresh();
    final long oneHourMillis = 3600 * 1000;
    Mockito.doReturn(System.currentTimeMillis() - oneHourMillis)
        .when(provider).getTokenFetchTime();

    assertTrue(provider.hasEnoughTimeElapsedSinceLastRefresh());
  }

  /**
   * Test that the token will not expire before one hour after the last refresh.
   */
  @Test
  public void testTokenDoesNotExpireTooSoon() {
    WorkloadIdentityTokenProvider provider = Mockito.spy(
        new WorkloadIdentityTokenProvider(AUTHORITY, TENANT_ID, CLIENT_ID, TOKEN_FILE));

    Mockito.doCallRealMethod().when(provider).hasEnoughTimeElapsedSinceLastRefresh();
    final long fiftyNineMinutesMillis = 59 * 60 * 1000;
    Mockito.doReturn(System.currentTimeMillis() - fiftyNineMinutesMillis)
        .when(provider).getTokenFetchTime();

    assertFalse(provider.hasEnoughTimeElapsedSinceLastRefresh());
  }
}

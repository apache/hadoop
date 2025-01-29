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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the refresh logic of workload identity tokens.
 */
public class TestWorkloadIdentityTokenProvider extends AbstractAbfsTestWithTimeout {

  private static final String AUTHORITY = "authority";
  private static final String TENANT_ID = "00000000-0000-0000-0000-000000000000";
  private static final String CLIENT_ID = "00000000-0000-0000-0000-000000000000";
  private static final String TOKEN_FILE = "/tmp/does_not_exist";
  private static final String CLIENT_ASSERTION = "dummy-client-assertion";
  private static final String TOKEN = "dummy-token";
  private static final long FEW_SECONDS = 5 * 1000;
  private static final long ONE_MINUTE = 60 * 1000;
  private static final long FIVE_MINUTES = 5 * ONE_MINUTE;

  public TestWorkloadIdentityTokenProvider() {
  }

  /**
   * Test that the token starts as expired.
   */
  @Test
  public void testTokenStartsAsExpired() {
    WorkloadIdentityTokenProvider provider = new WorkloadIdentityTokenProvider(
        AUTHORITY, TENANT_ID, CLIENT_ID, TOKEN_FILE);

    Assertions.assertThat(provider.isTokenAboutToExpire())
        .describedAs("Token should start as expired")
        .isTrue();
  }

  @Test
  public void testTokenFetchAndExpiry() throws Exception{
    long startTime = System.currentTimeMillis();
    AzureADToken adToken = new AzureADToken();
    adToken.setAccessToken(TOKEN);
    adToken.setExpiry(new Date(System.currentTimeMillis() + FEW_SECONDS + FIVE_MINUTES));

    File tokenFile = File.createTempFile(TOKEN_FILE, "txt");
    FileUtils.write(tokenFile, CLIENT_ASSERTION, StandardCharsets.UTF_8);

    WorkloadIdentityTokenProvider mockedTokenProvider = Mockito.spy(
        new WorkloadIdentityTokenProvider(AUTHORITY, TENANT_ID, CLIENT_ID,
            tokenFile.getPath()));
    Mockito.doReturn(adToken).when(mockedTokenProvider).getTokenUsingJWTAssertion(CLIENT_ASSERTION);

    // Token should be expired first and fetched
    Assertions.assertThat(mockedTokenProvider.isTokenAboutToExpire())
        .describedAs("Token should not be expired")
        .isTrue();
    Assertions.assertThat(mockedTokenProvider.getToken().getAccessToken())
        .describedAs("Token should be fetched")
        .isEqualTo(TOKEN);
    Assertions.assertThat(mockedTokenProvider.getTokenFetchTime())
        .describedAs("Token should not be expired")
        .isGreaterThan(startTime);

    // Token should be valid for few seconds.
    Assertions.assertThat(mockedTokenProvider.isTokenAboutToExpire())
        .describedAs("Token should not be expired")
        .isFalse();

    // Token should be expired after few seconds.
    Thread.sleep(FEW_SECONDS);
    Assertions.assertThat(mockedTokenProvider.isTokenAboutToExpire())
        .describedAs("Token should be expired")
        .isTrue();
  }

  /**
   * Test that an exception is thrown when the token file is empty.
   *
   * @throws IOException if file I/O fails.
   */
  @Test
  public void testTokenFetchWithEmptyTokenFile() throws Exception {
    File tokenFile = File.createTempFile("azure-identity-token", "txt");
    AzureADToken azureAdToken = new AzureADToken();
    WorkloadIdentityTokenProvider tokenProvider = Mockito.spy(
        new WorkloadIdentityTokenProvider(AUTHORITY, TENANT_ID, CLIENT_ID, tokenFile.getPath()));
    Mockito.doReturn(azureAdToken)
        .when(tokenProvider).getTokenUsingJWTAssertion(TOKEN);
    IOException ex = intercept(IOException.class, () -> {
      tokenProvider.getToken();
    });
    Assertions.assertThat(ex.getMessage())
      .describedAs("Exception should be thrown when the token file is empty")
      .contains("Empty token file");
  }

  /**
   * Test that an exception is thrown when the token file is not present.
   *
   * @throws IOException if file I/O fails.
   */
  @Test
  public void testTokenFetchWithTokenFileNotFound() throws Exception {
    AzureADToken azureAdToken = new AzureADToken();
    WorkloadIdentityTokenProvider tokenProvider = Mockito.spy(
        new WorkloadIdentityTokenProvider(AUTHORITY, TENANT_ID, CLIENT_ID, TOKEN_FILE));
    Mockito.doReturn(azureAdToken)
        .when(tokenProvider).getTokenUsingJWTAssertion(TOKEN);
    IOException ex = intercept(IOException.class, () -> {
      tokenProvider.getToken();
    });
    Assertions.assertThat(ex.getMessage())
        .describedAs("Exception should be thrown when the token file not found")
        .contains("Error reading token file");
  }
}

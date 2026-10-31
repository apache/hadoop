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

import java.util.Date;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the passthrough logic for tokens passed in from outside hadoop-azure.
 */
public class TestPassthroughTokenProvider extends AbstractAbfsTestWithTimeout {

    private static final String TOKEN = "dummy-token";

    public TestPassthroughTokenProvider() {
    }

    /**
     * Verify constructor validates token parameter.
     */
    @Test
    public void testConstructorRejectsNullToken() throws Exception {
        Throwable ex = intercept(RuntimeException.class, () -> {
            new PassthroughTokenProvider(null, 0);
        });
        Assertions.assertThat(ex.getMessage())
                .describedAs("Should validate token parameter")
                .contains("token");
    }

    /**
     * Verify refreshToken returns the token passed in and sets expiry based on expires_on seconds.
     */
    @Test
    public void testRefreshTokenReturnsPassthroughTokenAndExpiry() throws Exception {
        int expiresOnSeconds = (int) (System.currentTimeMillis() / 1000L) + 300; // +5 minutes
        PassthroughTokenProvider provider =
                new PassthroughTokenProvider(TOKEN, expiresOnSeconds);

        AzureADToken adToken = provider.getToken();

        Assertions.assertThat(adToken)
                .describedAs("Token should be returned")
                .isNotNull();
        Assertions.assertThat(adToken.getAccessToken())
                .describedAs("Access token should match the passthrough token")
                .isEqualTo(TOKEN);

        Date expectedExpiry = new Date(expiresOnSeconds * 1000L);
        Assertions.assertThat(adToken.getExpiry())
                .describedAs("Expiry should be derived from expiresOn seconds")
                .isEqualTo(expectedExpiry);
    }

    /**
     * Verify provider caches the refreshed token, so multiple getToken() calls
     * return the same token value and same expiry (no change).
     */
    @Test
    public void testGetTokenIsStableAcrossCalls() throws Exception {
        int expiresOnSeconds = (int) (System.currentTimeMillis() / 1000L) + 300; // +5 minutes
        PassthroughTokenProvider provider =
                new PassthroughTokenProvider(TOKEN, expiresOnSeconds);

        AzureADToken t1 = provider.getToken();
        AzureADToken t2 = provider.getToken();

        Assertions.assertThat(t2.getAccessToken())
                .describedAs("Access token should remain stable across calls")
                .isEqualTo(TOKEN);
        Assertions.assertThat(t2.getExpiry())
                .describedAs("Expiry should remain stable across calls")
                .isEqualTo(t1.getExpiry());
    }

    /**
     * Verify expiry is set correctly for an epoch-based expires_on value.
     */
    @Test
    public void testExpiryUsesEpochSeconds() throws Exception {
        int expiresOnSeconds = 12345;
        PassthroughTokenProvider provider =
                new PassthroughTokenProvider(TOKEN, expiresOnSeconds);

        AzureADToken adToken = provider.getToken();

        Assertions.assertThat(adToken.getExpiry())
                .describedAs("Expiry should be epoch seconds converted to milliseconds")
                .isEqualTo(new Date(12345L * 1000L));
    }
}

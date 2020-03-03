/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.unittests;

import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertThat;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.reflect.Whitebox.getInternalState;
import static org.powermock.reflect.Whitebox.invokeMethod;
import static org.powermock.reflect.Whitebox.setInternalState;

/**
 * Unit test for MsiTokenProvider
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AzureADAuthenticator.class)
public class TestMsiTokenProvider {

  private static final long ONE_HOUR = 3600 * 1000;
  private static final long TWO_HOUR = ONE_HOUR * 2;

  @Test
  public void testMsiTokenProvider() throws Exception {
    String testToken = "TEST_TOKEN1";
    setMockAzureADAuthenticator(testToken,
        System.currentTimeMillis() + TWO_HOUR);

    AccessTokenProvider msiTokenProvider = new MsiTokenProvider("", "", "", "");
    long tokenFetchTime = getInternalState(msiTokenProvider, "tokenFetchTime");
    Assert.assertEquals(-1, tokenFetchTime);

    long before = System.currentTimeMillis();
    AzureADToken token = msiTokenProvider.getToken();
    long after = System.currentTimeMillis();
    long newTokenFetchTime = getInternalState(msiTokenProvider,
        "tokenFetchTime");
    assertThat(newTokenFetchTime,
        is(allOf(greaterThan(before), lessThanOrEqualTo(after))));
    assertThat(token.getAccessToken(), is(equalTo(testToken)));

    assertThat(invokeMethod(msiTokenProvider, "isTokenAboutToExpire"),
        is(false));

    setInternalState(msiTokenProvider, "tokenFetchTime",
        System.currentTimeMillis() - ONE_HOUR);
    assertThat(invokeMethod(msiTokenProvider, "isTokenAboutToExpire"),
        is(true));

    setInternalState(msiTokenProvider, "tokenFetchTime",
        System.currentTimeMillis() + 2 - ONE_HOUR);
    assertThat(invokeMethod(msiTokenProvider, "isTokenAboutToExpire"),
        is(false));
  }

  private AzureADToken setMockAzureADAuthenticator(String tokenStr, long expiry)
      throws IOException {
    AzureADToken token = new AzureADToken();
    token.setAccessToken(tokenStr);
    token.setExpiry(new Date(expiry));
    mockStatic(AzureADAuthenticator.class);
    when(AzureADAuthenticator
        .getTokenFromMsi(Mockito.anyString(), Mockito.anyString(),
            Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean()))
        .thenReturn(token);
    return token;
  }

}

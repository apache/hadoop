/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import com.squareup.okhttp.mockwebserver.MockResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.common.CustomMockTokenProvider;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_BLOCK_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_CLASS_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_TYPE_KEY;

/**
 * Test access token provider behaviour with custom token provider and for token
 * provider cache is enabled.
 */
@RunWith(Parameterized.class)
public class TestCustomTokenProvider extends AdlMockWebServer {
  private static final long TEN_MINUTES_IN_MILIS = 600000;
  private int backendCallCount;
  private int expectedCallbackToAccessToken;
  private TestableAdlFileSystem[] fileSystems;
  private Class typeOfTokenProviderClass;
  private long expiryFromNow;
  private int fsObjectCount;

  public TestCustomTokenProvider(Class typeOfTokenProviderClass,
      long expiryFromNow, int fsObjectCount, int backendCallCount,
      int expectedCallbackToAccessToken)
      throws IllegalAccessException, InstantiationException, URISyntaxException,
      IOException {
    this.typeOfTokenProviderClass = typeOfTokenProviderClass;
    this.expiryFromNow = expiryFromNow;
    this.fsObjectCount = fsObjectCount;
    this.backendCallCount = backendCallCount;
    this.expectedCallbackToAccessToken = expectedCallbackToAccessToken;
  }

  @Parameterized.Parameters(name = "{index}")
  public static Collection testDataForTokenProvider() {
    return Arrays.asList(new Object[][] {
        // Data set in order
        // INPUT - CustomTokenProvider class to load
        // INPUT - expiry time in milis. Subtract from current time
        // INPUT - No. of FileSystem object
        // INPUT - No. of backend calls per FileSystem object
        // EXPECTED - Number of callbacks to get token after test finished.
        {CustomMockTokenProvider.class, 0, 1, 1, 1},
        {CustomMockTokenProvider.class, TEN_MINUTES_IN_MILIS, 1, 1, 1},
        {CustomMockTokenProvider.class, TEN_MINUTES_IN_MILIS, 2, 1, 2},
        {CustomMockTokenProvider.class, TEN_MINUTES_IN_MILIS, 10, 10, 10}});
  }

  /**
   * Explicitly invoked init so that base class mock server is setup before
   * test data initialization is done.
   *
   * @throws IOException
   * @throws URISyntaxException
   */
  public void init() throws IOException, URISyntaxException {
    Configuration configuration = new Configuration();
    configuration.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY,
        TokenProviderType.Custom);
    configuration.set(AZURE_AD_TOKEN_PROVIDER_CLASS_KEY,
        typeOfTokenProviderClass.getName());
    fileSystems = new TestableAdlFileSystem[fsObjectCount];
    URI uri = new URI("adl://localhost:" + getPort());

    for (int i = 0; i < fsObjectCount; ++i) {
      fileSystems[i] = new TestableAdlFileSystem();
      fileSystems[i].initialize(uri, configuration);

      ((CustomMockTokenProvider) fileSystems[i].getAzureTokenProvider())
          .setExpiryTimeInMillisAfter(expiryFromNow);
    }
  }

  @Test
  public void testCustomTokenManagement()
      throws IOException, URISyntaxException {
    int accessTokenCallbackDuringExec = 0;
    init();
    for (TestableAdlFileSystem tfs : fileSystems) {
      for (int i = 0; i < backendCallCount; ++i) {
        getMockServer().enqueue(new MockResponse().setResponseCode(200)
            .setBody(TestADLResponseData.getGetFileStatusJSONResponse()));
        FileStatus fileStatus = tfs.getFileStatus(new Path("/test1/test2"));
        Assert.assertTrue(fileStatus.isFile());
        Assert.assertEquals("adl://" + getMockServer().getHostName() + ":" +
            getMockServer().getPort() + "/test1/test2",
            fileStatus.getPath().toString());
        Assert.assertEquals(4194304, fileStatus.getLen());
        Assert.assertEquals(ADL_BLOCK_SIZE, fileStatus.getBlockSize());
        Assert.assertEquals(1, fileStatus.getReplication());
        Assert
            .assertEquals(new FsPermission("777"), fileStatus.getPermission());
        Assert.assertEquals("NotSupportYet", fileStatus.getOwner());
        Assert.assertEquals("NotSupportYet", fileStatus.getGroup());
      }

      accessTokenCallbackDuringExec += ((CustomMockTokenProvider) tfs
          .getAzureTokenProvider()).getAccessTokenRequestCount();
    }

    Assert.assertEquals(expectedCallbackToAccessToken,
        accessTokenCallbackDuringExec);
  }
}

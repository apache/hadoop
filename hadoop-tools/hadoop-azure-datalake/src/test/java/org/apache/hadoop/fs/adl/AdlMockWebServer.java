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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.common.CustomMockTokenProvider;
import org.apache.hadoop.fs.adl.oauth2.AzureADTokenProvider;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_CLASS_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .AZURE_AD_TOKEN_PROVIDER_TYPE_KEY;

import com.squareup.okhttp.mockwebserver.MockWebServer;

import org.junit.After;
import org.junit.Before;

/**
 * Mock server to simulate Adls backend calls. This infrastructure is expandable
 * to override expected server response based on the derived test functionality.
 * Common functionality to generate token information before request is send to
 * adls backend is also managed within AdlMockWebServer implementation using
 * {@link org.apache.hadoop.fs.adl.common.CustomMockTokenProvider}.
 */
public class AdlMockWebServer {
  // Create a MockWebServer. These are lean enough that you can create a new
  // instance for every unit test.
  private MockWebServer server = null;
  private TestableAdlFileSystem fs = null;
  private int port = 0;
  private Configuration conf = new Configuration();

  public MockWebServer getMockServer() {
    return server;
  }

  public TestableAdlFileSystem getMockAdlFileSystem() {
    return fs;
  }

  public int getPort() {
    return port;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Before
  public void preTestSetup() throws IOException, URISyntaxException {
    server = new MockWebServer();

    // Start the server.
    server.start();

    // Ask the server for its URL. You'll need this to make HTTP requests.
    URL baseUrl = server.getUrl("");
    port = baseUrl.getPort();

    // Exercise your application code, which should make those HTTP requests.
    // Responses are returned in the same order that they are enqueued.
    fs = new TestableAdlFileSystem();

    conf.setEnum(AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, TokenProviderType.Custom);
    conf.setClass(AZURE_AD_TOKEN_PROVIDER_CLASS_KEY,
        CustomMockTokenProvider.class, AzureADTokenProvider.class);

    URI uri = new URI("adl://localhost:" + port);
    fs.initialize(uri, conf);
  }

  @After
  public void postTestSetup() throws IOException {
    fs.close();
    server.shutdown();
  }
}

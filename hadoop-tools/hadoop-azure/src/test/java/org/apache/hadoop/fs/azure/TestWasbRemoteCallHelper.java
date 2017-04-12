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

package org.apache.hadoop.fs.azure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.KEY_USE_SECURE_MODE;

/**
 * Test class to hold all WasbRemoteCallHelper tests
 */
public class TestWasbRemoteCallHelper
    extends AbstractWasbTestBase {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();
    conf.set(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, "true");
    conf.set(RemoteWasbAuthorizerImpl.KEY_REMOTE_AUTH_SERVICE_URL, "http://localhost/");
    return AzureBlobStorageTestAccount.create(conf);
  }

  @Before
  public void beforeMethod() {
    boolean useSecureMode = fs.getConf().getBoolean(KEY_USE_SECURE_MODE, false);
    boolean useAuthorization = fs.getConf().getBoolean(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, false);
    Assume.assumeTrue("Test valid when both SecureMode and Authorization are enabled .. skipping",
        useSecureMode && useAuthorization);

    Assume.assumeTrue(
        useSecureMode && useAuthorization
    );
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  /**
   * Test invalid status-code
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testInvalidStatusCode() throws Throwable {

    setupExpectations();

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(999));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test invalid Content-Type
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testInvalidContentType() throws Throwable {

    setupExpectations();

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(200));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "text/plain"));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test missing Content-Length
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testMissingContentLength() throws Throwable {

    setupExpectations();

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(200));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test Content-Length exceeds max
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testContentLengthExceedsMax() throws Throwable {

    setupExpectations();

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(200));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "2048"));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test invalid Content-Length value
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testInvalidContentLengthValue() throws Throwable {

    setupExpectations();

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(200));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "20abc48"));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test valid JSON response
   * @throws Throwable
   */
  @Test
  public void testValidJSONResponse() throws Throwable {

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);

    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(200));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    Mockito.when(mockHttpEntity.getContent())
        .thenReturn(new ByteArrayInputStream(validJsonResponse().getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(validJsonResponse().getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(validJsonResponse().getBytes(StandardCharsets.UTF_8)));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test malformed JSON response
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testMalFormedJSONResponse() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("com.fasterxml.jackson.core.JsonParseException: Unexpected end-of-input in FIELD_NAME");

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);

    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(200));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    Mockito.when(mockHttpEntity.getContent())
        .thenReturn(new ByteArrayInputStream(malformedJsonResponse().getBytes(StandardCharsets.UTF_8)));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test valid JSON response failure response code
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testFailureCodeJSONResponse() throws Throwable {

    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("Remote authorization service encountered an error Unauthorized");

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);

    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(200));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    Mockito.when(mockHttpEntity.getContent())
        .thenReturn(new ByteArrayInputStream(failureCodeJsonResponse().getBytes(StandardCharsets.UTF_8)));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  private void setupExpectations() {
    expectedEx.expect(WasbAuthorizationException.class);
    expectedEx.expectMessage("org.apache.hadoop.fs.azure.WasbRemoteCallException: "
        + "http://localhost/CHECK_AUTHORIZATION?wasb_absolute_path=%2F&"
        + "operation_type=write:Encountered IOException while making remote call");
  }

  private void performop(HttpClient mockHttpClient) throws Throwable {
    AzureBlobStorageTestAccount testAccount = createTestAccount();
    NativeAzureFileSystem fs = testAccount.getFileSystem();

    Path testPath = new Path("/", "test.dat");

    RemoteWasbAuthorizerImpl authorizer = new RemoteWasbAuthorizerImpl();
    authorizer.init(fs.getConf());
    WasbRemoteCallHelper mockWasbRemoteCallHelper = new WasbRemoteCallHelper();
    mockWasbRemoteCallHelper.updateHttpClient(mockHttpClient);
    authorizer.updateWasbRemoteCallHelper(mockWasbRemoteCallHelper);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(testPath);
    ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
    fs.delete(testPath, false);
  }

  private String validJsonResponse() {
    return new String(
        "{\"responseCode\": 0, \"authorizationResult\": true, \"responseMessage\": \"Authorized\"}"
    );
  }

  private String malformedJsonResponse() {
    return new String(
        "{\"responseCode\": 0, \"authorizationResult\": true, \"responseMessage\":"
    );
  }

  private String failureCodeJsonResponse() {
    return new String(
        "{\"responseCode\": 1, \"authorizationResult\": false, \"responseMessage\": \"Unauthorized\"}"
    );
  }

  private StatusLine newStatusLine(int statusCode) {
    return new StatusLine() {
      @Override
      public ProtocolVersion getProtocolVersion() {
        return new ProtocolVersion("HTTP", 1, 1);
      }

      @Override
      public int getStatusCode() {
        return statusCode;
      }

      @Override
      public String getReasonPhrase() {
        return "Reason Phrase";
      }
    };
  }

  private Header newHeader(String name, String value) {
    return new Header() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getValue() {
        return value;
      }

      @Override
      public HeaderElement[] getElements() throws ParseException {
        return new HeaderElement[0];
      }
    };
  }
}
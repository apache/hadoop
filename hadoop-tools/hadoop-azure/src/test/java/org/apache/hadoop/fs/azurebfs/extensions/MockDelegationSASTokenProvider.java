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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.DelegationSASGenerator;
import org.apache.hadoop.fs.azurebfs.utils.SASGenerator;
import org.apache.hadoop.security.AccessControlException;

/**
 * A mock SAS token provider implementation
 */
public class MockDelegationSASTokenProvider implements SASTokenProvider {

  private DelegationSASGenerator generator;

  public static final String TEST_OWNER = "325f1619-4205-432f-9fce-3fd594325ce5";
  public static final String CORRELATION_ID = "66ff4ffc-ff17-417e-a2a9-45db8c5b0b5c";
  public static final String NO_AGENT_PATH = "NoAgentPath";

  @Override
  public void initialize(Configuration configuration, String accountName) throws IOException {
    String appID = configuration.get(TestConfigurationKeys.FS_AZURE_TEST_APP_ID);
    String appSecret = configuration.get(TestConfigurationKeys.FS_AZURE_TEST_APP_SECRET);
    String sktid = configuration.get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_TENANT_ID);
    String skoid = configuration.get(TestConfigurationKeys.FS_AZURE_TEST_APP_SERVICE_PRINCIPAL_OBJECT_ID);
    String skt = SASGenerator.ISO_8601_FORMATTER.format(Instant.now().minus(SASGenerator.FIVE_MINUTES));
    String ske = SASGenerator.ISO_8601_FORMATTER.format(Instant.now().plus(SASGenerator.ONE_DAY));
    String skv = SASGenerator.AuthenticationVersion.Dec19.toString();

    byte[] key = getUserDelegationKey(accountName, appID, appSecret, sktid, skt, ske, skv);

    generator = new DelegationSASGenerator(key, skoid, sktid, skt, ske, skv);
  }

  // Invokes the AAD v2.0 authentication endpoint with a client credentials grant to get an
  // access token.  See https://docs.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow.
  private String getAuthorizationHeader(String accountName, String appID, String appSecret, String sktid) throws IOException {
    String authEndPoint = String.format("https://login.microsoftonline.com/%s/oauth2/v2.0/token", sktid);
    ClientCredsTokenProvider provider = new ClientCredsTokenProvider(authEndPoint, appID, appSecret);
    return "Bearer " + provider.getToken().getAccessToken();
  }

  private byte[] getUserDelegationKey(String accountName, String appID, String appSecret,
      String sktid, String skt, String ske, String skv) throws IOException {

    String method = "POST";
    String account = accountName.substring(0, accountName.indexOf(AbfsHttpConstants.DOT));

    final StringBuilder sb = new StringBuilder(128);
    sb.append("https://");
    sb.append(account);
    sb.append(".blob.core.windows.net/?restype=service&comp=userdelegationkey");

    URL url;
    try {
      url = new URL(sb.toString());
    } catch (MalformedURLException ex) {
      throw new InvalidUriException(sb.toString());
    }

    List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_VERSION, skv));
    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.CONTENT_TYPE, "application/x-www-form-urlencoded"));
    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.AUTHORIZATION, getAuthorizationHeader(account, appID, appSecret, sktid)));

    final StringBuilder requestBody = new StringBuilder(512);
    requestBody.append("<?xml version=\"1.0\" encoding=\"utf-8\"?><KeyInfo><Start>");
    requestBody.append(skt);
    requestBody.append("</Start><Expiry>");
    requestBody.append(ske);
    requestBody.append("</Expiry></KeyInfo>");

    AbfsHttpOperation op = new AbfsHttpOperation(url, method, requestHeaders);

    byte[] requestBuffer = requestBody.toString().getBytes(StandardCharsets.UTF_8.toString());
    op.sendRequest(requestBuffer, 0, requestBuffer.length);

    byte[] responseBuffer = new byte[4 * 1024];
    op.processResponse(responseBuffer, 0, responseBuffer.length);

    String responseBody = new String(responseBuffer, 0, (int) op.getBytesReceived(), StandardCharsets.UTF_8);
    int beginIndex = responseBody.indexOf("<Value>") + "<Value>".length();
    int endIndex = responseBody.indexOf("</Value>");
    String value = responseBody.substring(beginIndex, endIndex);
    return Base64.decode(value);
  }

  /**
   * Invokes the authorizer to obtain a SAS token.
   *
   * @param accountName the name of the storage account.
   * @param fileSystem the name of the fileSystem.
   * @param path the file or directory path.
   * @param operation the operation to be performed on the path.
   * @return a SAS token to perform the request operation.
   * @throws IOException if there is a network error.
   * @throws AccessControlException if access is denied.
   */
  @Override
  public String getSASToken(String accountName, String fileSystem, String path,
                     String operation) throws IOException, AccessControlException {
    // Except for the special case where we test without an agent,
    // the user for these tests is always TEST_OWNER.  The check access operation
    // requires suoid to check permissions for the user and will throw if the
    // user does not have access and otherwise succeed.
    String saoid = null;
    String suoid = null;
    if (path == null || !path.endsWith(NO_AGENT_PATH)) {
      saoid = (operation == SASTokenProvider.CHECK_ACCESS_OPERATION) ? null : TEST_OWNER;
      suoid = (operation == SASTokenProvider.CHECK_ACCESS_OPERATION) ? TEST_OWNER : null;
    }
    return generator.getDelegationSAS(accountName, fileSystem, path, operation,
        saoid, suoid, CORRELATION_ID);
  }
}
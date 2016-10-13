/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hdfs.web.oauth2;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.ObjectReader;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.MediaType;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.util.Timer;
import org.apache.http.HttpStatus;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.web.oauth2.Utils.notNull;


/**
 * Obtain an access token via the credential-based OAuth2 workflow.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AzureADClientCredentialBasedAccesTokenProvider
    extends AccessTokenProvider {
  private static final ObjectReader READER =
      new ObjectMapper().reader(Map.class);

  public static final String OAUTH_CREDENTIAL_KEY
      = "dfs.webhdfs.oauth2.credential";

  public static final String AAD_RESOURCE_KEY
      = "fs.adls.oauth2.resource";

  public static final String RESOURCE_PARAM_NAME
      = "resource";

  private static final String OAUTH_CLIENT_ID_KEY
      = "dfs.webhdfs.oauth2.client.id";

  private static final String OAUTH_REFRESH_URL_KEY
      = "dfs.webhdfs.oauth2.refresh.url";


  public static final String ACCESS_TOKEN = "access_token";
  public static final String CLIENT_CREDENTIALS = "client_credentials";
  public static final String CLIENT_ID = "client_id";
  public static final String CLIENT_SECRET = "client_secret";
  public static final String EXPIRES_IN = "expires_in";
  public static final String GRANT_TYPE = "grant_type";
  public static final MediaType URLENCODED
          = MediaType.parse("application/x-www-form-urlencoded; charset=utf-8");


  private AccessTokenTimer timer;

  private String clientId;

  private String refreshURL;

  private String accessToken;

  private String resource;

  private String credential;

  private boolean initialCredentialObtained = false;

  AzureADClientCredentialBasedAccesTokenProvider() {
    this.timer = new AccessTokenTimer();
  }

  AzureADClientCredentialBasedAccesTokenProvider(Timer timer) {
    this.timer = new AccessTokenTimer(timer);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    clientId = notNull(conf, OAUTH_CLIENT_ID_KEY);
    refreshURL = notNull(conf, OAUTH_REFRESH_URL_KEY);
    resource = notNull(conf, AAD_RESOURCE_KEY);
    credential = notNull(conf, OAUTH_CREDENTIAL_KEY);
  }

  @Override
  public String getAccessToken() throws IOException {
    if(timer.shouldRefresh() || !initialCredentialObtained) {
      refresh();
      initialCredentialObtained = true;
    }
    return accessToken;
  }

  void refresh() throws IOException {
    try {
      OkHttpClient client = new OkHttpClient();
      client.setConnectTimeout(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
          TimeUnit.MILLISECONDS);
      client.setReadTimeout(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
          TimeUnit.MILLISECONDS);

      String bodyString = Utils.postBody(CLIENT_SECRET, credential,
          GRANT_TYPE, CLIENT_CREDENTIALS,
          RESOURCE_PARAM_NAME, resource,
          CLIENT_ID, clientId);

      RequestBody body = RequestBody.create(URLENCODED, bodyString);

      Request request = new Request.Builder()
          .url(refreshURL)
          .post(body)
          .build();
      Response responseBody = client.newCall(request).execute();

      if (responseBody.code() != HttpStatus.SC_OK) {
        throw new IllegalArgumentException("Received invalid http response: "
            + responseBody.code() + ", text = " + responseBody.toString());
      }

      Map<?, ?> response = READER.readValue(responseBody.body().string());

      String newExpiresIn = response.get(EXPIRES_IN).toString();
      timer.setExpiresIn(newExpiresIn);

      accessToken = response.get(ACCESS_TOKEN).toString();

    } catch (Exception e) {
      throw new IOException("Unable to obtain access token from credential", e);
    }
  }
}

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.util.JsonSerialization;
import org.apache.hadoop.util.Timer;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_CLIENT_ID_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.CLIENT_CREDENTIALS;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.CLIENT_ID;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.CLIENT_SECRET;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.EXPIRES_IN;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.GRANT_TYPE;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.URLENCODED;
import static org.apache.hadoop.hdfs.web.oauth2.Utils.notNull;

/**
 * Obtain an access token via the credential-based OAuth2 workflow.  This
 * abstract class requires only that implementations provide the credential,
 * which the class then uses to obtain a refresh token.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class CredentialBasedAccessTokenProvider
    extends AccessTokenProvider {

  public static final String OAUTH_CREDENTIAL_KEY
      = "dfs.webhdfs.oauth2.credential";

  private AccessTokenTimer timer;

  private String clientId;

  private String refreshURL;

  private String accessToken;

  private boolean initialCredentialObtained = false;

  CredentialBasedAccessTokenProvider() {
    this.timer = new AccessTokenTimer();
  }

  CredentialBasedAccessTokenProvider(Timer timer) {
    this.timer = new AccessTokenTimer(timer);
  }

  public abstract String getCredential();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    clientId = notNull(conf, OAUTH_CLIENT_ID_KEY);
    refreshURL = notNull(conf, OAUTH_REFRESH_URL_KEY);
  }

  @Override
  public synchronized String getAccessToken() throws IOException {
    if(timer.shouldRefresh() || !initialCredentialObtained) {
      refresh();
      initialCredentialObtained = true;
    }

    return accessToken;
  }

  void refresh() throws IOException {
    final List<NameValuePair> pairs = new ArrayList<>();
    pairs.add(new BasicNameValuePair(CLIENT_SECRET, getCredential()));
    pairs.add(new BasicNameValuePair(GRANT_TYPE, CLIENT_CREDENTIALS));
    pairs.add(new BasicNameValuePair(CLIENT_ID, clientId));
    final RequestConfig config = RequestConfig.custom()
        .setConnectTimeout(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT)
        .setConnectionRequestTimeout(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT)
        .setSocketTimeout(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT)
        .build();
    try (CloseableHttpClient client =
             HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
      final HttpPost httpPost = new HttpPost(refreshURL);
      httpPost.setEntity(new UrlEncodedFormEntity(pairs, StandardCharsets.UTF_8));
      httpPost.setHeader(HttpHeaders.CONTENT_TYPE, URLENCODED);
      try (CloseableHttpResponse response = client.execute(httpPost)) {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
          throw new IllegalArgumentException(
              "Received invalid http response: " + statusCode + ", text = " +
                  EntityUtils.toString(response.getEntity()));
        }
        Map<?, ?> responseBody = JsonSerialization.mapReader().readValue(
            EntityUtils.toString(response.getEntity()));

        String newExpiresIn = responseBody.get(EXPIRES_IN).toString();
        timer.setExpiresIn(newExpiresIn);

        accessToken = responseBody.get(ACCESS_TOKEN).toString();
      }
    } catch (RuntimeException e) {
      throw new IOException("Unable to obtain access token from credential", e);
    } catch (Exception e) {
      throw new IOException("Unable to obtain access token from credential", e);
    }
  }
}

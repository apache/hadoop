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

package org.apache.hadoop.runc.docker.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.Credentials;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.auth.RFC2617Scheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

@SuppressWarnings("deprecation")
public class BearerScheme extends RFC2617Scheme {

  public static final String SCHEME = "bearer";
  private static final long serialVersionUID = -8061843510470788770L;
  private static final Logger LOG = LoggerFactory.getLogger(BearerScheme.class);
  private final CloseableHttpClient client;
  private volatile boolean complete = false;

  public BearerScheme(CloseableHttpClient client) {
    this.client = client;
  }

  @Override
  public String getSchemeName() {
    return SCHEME;
  }

  @Override
  public boolean isConnectionBased() {
    return false;
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public Header authenticate(Credentials credentials, HttpRequest request)
      throws AuthenticationException {

    // succeed or fail, don't come around again
    complete = true;

    if (!(credentials instanceof BearerCredentials)) {
      return null;
    }

    BearerCredentials bearerCreds = (BearerCredentials) credentials;
    if (bearerCreds.isValid()) {
      LOG.debug("Returning cached credentials");
      return new BasicHeader(HttpHeaders.AUTHORIZATION,
          String.format("Bearer %s", bearerCreds.getToken().getToken()));
    }

    authenticate(bearerCreds);
    if (bearerCreds.isValid()) {
      return new BasicHeader(HttpHeaders.AUTHORIZATION,
          String.format("Bearer %s", bearerCreds.getToken().getToken()));
    }

    return null;
  }

  private void authenticate(BearerCredentials creds)
      throws AuthenticationException {

    String realm = creds.getAuthScope().getRealm();
    URL url;
    try {
      url = new URL(realm);
    } catch (MalformedURLException e) {
      throw new AuthenticationException(
          String.format("Invalid realm: %s", realm), e);
    }

    LOG.debug("Authenticating to {}", url);

    HttpGet get = new HttpGet(url.toExternalForm());

    try (CloseableHttpResponse response = client.execute(get)) {

      if (response.getStatusLine().getStatusCode() != 200) {
        LOG.debug("Got unexpected response {} from token service {}",
            response.getStatusLine(), realm);
        return;
      }

      Header contentType = response.getFirstHeader("content-type");
      if (contentType != null && !contentType.getValue()
          .equals("application/json")) {
        LOG.debug("Got unexpected content type {} from token service {}",
            contentType.getValue(), realm);
        return;
      }

      HttpEntity entity = response.getEntity();
      if (entity == null) {
        LOG.debug("No token received from token service {}", realm);
        return;
      }

      try {
        AuthToken authToken = new ObjectMapper().readerFor(AuthToken.class)
            .readValue(entity.getContent());
        creds.setToken(authToken);
        LOG.debug("Authenticated succesfully");
      } finally {
        EntityUtils.consume(entity);
      }
    } catch (IOException e) {
      throw new AuthenticationException("Unable to acquire token", e);
    }
  }

  @Override
  public String getRealm() {
    String realm = super.getRealm();
    try {

      URIBuilder ub = new URIBuilder(realm);
      String service = getParameter("service");
      if (service != null) {
        ub.addParameter("service", service);
      }
      String scope = getParameter("scope");
      if (scope != null) {
        ub.addParameter("scope", scope);
      }
      return ub.build().toURL().toExternalForm();
    } catch (URISyntaxException | MalformedURLException e) {
      return realm;
    }
  }

}

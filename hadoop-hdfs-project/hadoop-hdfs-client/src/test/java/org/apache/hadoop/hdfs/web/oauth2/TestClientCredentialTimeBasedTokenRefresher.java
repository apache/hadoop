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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.util.Timer;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;
import org.mockserver.model.ParameterBody;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ACCESS_TOKEN_PROVIDER_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_CLIENT_ID_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.CLIENT_CREDENTIALS;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.CLIENT_ID;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.CLIENT_SECRET;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.EXPIRES_IN;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.GRANT_TYPE;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.TOKEN_TYPE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class TestClientCredentialTimeBasedTokenRefresher {
  public final static Header CONTENT_TYPE_APPLICATION_JSON
      = new Header("Content-Type", "application/json");

  public final static String CLIENT_ID_FOR_TESTING = "joebob";

  public Configuration buildConf(String credential, String tokenExpires,
                                 String clientId, String refreshURL) {
    // Configurations are simple enough that it's not worth mocking them out.
    Configuration conf = new Configuration();
    conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY,
        credential);
    conf.set(ACCESS_TOKEN_PROVIDER_KEY,
        ConfCredentialBasedAccessTokenProvider.class.getName());
    conf.set(OAUTH_CLIENT_ID_KEY, clientId);
    conf.set(OAUTH_REFRESH_URL_KEY, refreshURL);
    return conf;
  }

  @Test
  public void refreshUrlIsCorrect() throws IOException {
    final int PORT = ServerSocketUtil.getPort(0, 20);
    final String REFRESH_ADDRESS = "http://localhost:" + PORT + "/refresh";

    long tokenExpires = 0;

    Configuration conf = buildConf("myreallycoolcredential",
        Long.toString(tokenExpires),
        CLIENT_ID_FOR_TESTING,
        REFRESH_ADDRESS);

    Timer mockTimer = mock(Timer.class);
    when(mockTimer.now()).thenReturn(tokenExpires + 1000l);

    AccessTokenProvider credProvider =
        new ConfCredentialBasedAccessTokenProvider(mockTimer);
    credProvider.setConf(conf);
    
    // Build mock server to receive refresh request
    ClientAndServer mockServer  = startClientAndServer(PORT);

    HttpRequest expectedRequest = request()
        .withMethod("POST")
        .withPath("/refresh")
        .withBody( 
        // Note, OkHttp does not sort the param values, so we need to do
        // it ourselves via the ordering provided to ParameterBody...
            ParameterBody.params(
                Parameter.param(CLIENT_SECRET, "myreallycoolcredential"),
                Parameter.param(GRANT_TYPE, CLIENT_CREDENTIALS),
                Parameter.param(CLIENT_ID, CLIENT_ID_FOR_TESTING)
                ));

    MockServerClient mockServerClient = new MockServerClient("localhost", PORT);

    // https://tools.ietf.org/html/rfc6749#section-5.1
    Map<String, Object> map = new TreeMap<>();
    
    map.put(EXPIRES_IN, "0987654321");
    map.put(TOKEN_TYPE, "bearer");
    map.put(ACCESS_TOKEN, "new access token");

    ObjectMapper mapper = new ObjectMapper();
    
    HttpResponse resp = response()
        .withStatusCode(HttpStatus.SC_OK)
        .withHeaders(
            CONTENT_TYPE_APPLICATION_JSON
        )
        .withBody(mapper.writeValueAsString(map));

    mockServerClient
        .when(expectedRequest, exactly(1))
        .respond(resp);

    assertEquals("new access token", credProvider.getAccessToken());

    mockServerClient.verify(expectedRequest);

    mockServerClient.clear(expectedRequest);
    mockServer.stop();
  }
}

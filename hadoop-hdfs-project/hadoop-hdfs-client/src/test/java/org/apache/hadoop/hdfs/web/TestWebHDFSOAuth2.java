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
package org.apache.hadoop.hdfs.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.oauth2.ConfCredentialBasedAccessTokenProvider;
import org.apache.hadoop.hdfs.web.oauth2.CredentialBasedAccessTokenProvider;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2ConnectionConfigurator;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ACCESS_TOKEN_PROVIDER_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_CLIENT_ID_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.EXPIRES_IN;
import static org.apache.hadoop.hdfs.web.oauth2.OAuth2Constants.TOKEN_TYPE;
import static org.junit.Assert.assertEquals;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class TestWebHDFSOAuth2 {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestWebHDFSOAuth2.class);

  private ClientAndServer mockWebHDFS;
  private ClientAndServer mockOAuthServer;

  public final static int WEBHDFS_PORT = 7552;
  public final static int OAUTH_PORT = 7553;

  public final static Header CONTENT_TYPE_APPLICATION_JSON = new Header("Content-Type", "application/json");

  public final static String AUTH_TOKEN = "0123456789abcdef";
  public final static Header AUTH_TOKEN_HEADER = new Header("AUTHORIZATION", OAuth2ConnectionConfigurator.HEADER + AUTH_TOKEN);

  @Before
  public void startMockOAuthServer() {
    mockOAuthServer = startClientAndServer(OAUTH_PORT);
  }
  @Before
  public void startMockWebHDFSServer() {
    System.setProperty("hadoop.home.dir", System.getProperty("user.dir"));

    mockWebHDFS = startClientAndServer(WEBHDFS_PORT);
  }

  @Test
  public void listStatusReturnsAsExpected() throws URISyntaxException, IOException {
    MockServerClient mockWebHDFSServerClient = new MockServerClient("localhost", WEBHDFS_PORT);
    MockServerClient mockOAuthServerClient = new MockServerClient("localhost", OAUTH_PORT);

    HttpRequest oauthServerRequest = getOAuthServerMockRequest(mockOAuthServerClient);

    HttpRequest fileSystemRequest = request()
        .withMethod("GET")
        .withPath(WebHdfsFileSystem.PATH_PREFIX + "/test1/test2")
        .withHeader(AUTH_TOKEN_HEADER);

    try {
      mockWebHDFSServerClient.when(fileSystemRequest,
          exactly(1)
      )
          .respond(
              response()
                  .withStatusCode(HttpStatus.SC_OK)
                  .withHeaders(
                      CONTENT_TYPE_APPLICATION_JSON
                  )
                  .withBody("{\n" +
                      "  \"FileStatuses\":\n" +
                      "  {\n" +
                      "    \"FileStatus\":\n" +
                      "    [\n" +
                      "      {\n" +
                      "        \"accessTime\"      : 1320171722771,\n" +
                      "        \"blockSize\"       : 33554432,\n" +
                      "        \"group\"           : \"supergroup\",\n" +
                      "        \"length\"          : 24930,\n" +
                      "        \"modificationTime\": 1320171722771,\n" +
                      "        \"owner\"           : \"webuser\",\n" +
                      "        \"pathSuffix\"      : \"a.patch\",\n" +
                      "        \"permission\"      : \"644\",\n" +
                      "        \"replication\"     : 1,\n" +
                      "        \"type\"            : \"FILE\"\n" +
                      "      },\n" +
                      "      {\n" +
                      "        \"accessTime\"      : 0,\n" +
                      "        \"blockSize\"       : 0,\n" +
                      "        \"group\"           : \"supergroup\",\n" +
                      "        \"length\"          : 0,\n" +
                      "        \"modificationTime\": 1320895981256,\n" +
                      "        \"owner\"           : \"szetszwo\",\n" +
                      "        \"pathSuffix\"      : \"bar\",\n" +
                      "        \"permission\"      : \"711\",\n" +
                      "        \"replication\"     : 0,\n" +
                      "        \"type\"            : \"DIRECTORY\"\n" +
                      "      }\n" +
                      "    ]\n" +
                      "  }\n" +
                      "}\n")
          );

      FileSystem fs = new WebHdfsFileSystem();
      Configuration conf = getConfiguration();
      conf.set(OAUTH_REFRESH_URL_KEY, "http://localhost:" + OAUTH_PORT + "/refresh");
      conf.set(CredentialBasedAccessTokenProvider.OAUTH_CREDENTIAL_KEY, "credential");

      URI uri = new URI("webhdfs://localhost:" + WEBHDFS_PORT);
      fs.initialize(uri, conf);

      FileStatus[] ls = fs.listStatus(new Path("/test1/test2"));

      mockOAuthServer.verify(oauthServerRequest);
      mockWebHDFSServerClient.verify(fileSystemRequest);

      assertEquals(2, ls.length);
      assertEquals("a.patch", ls[0].getPath().getName());
      assertEquals("bar", ls[1].getPath().getName());

      fs.close();
    } finally {
      mockWebHDFSServerClient.clear(fileSystemRequest);
      mockOAuthServerClient.clear(oauthServerRequest);
    }
  }

  private HttpRequest getOAuthServerMockRequest(MockServerClient mockServerClient) throws IOException {
    HttpRequest expectedRequest = request()
        .withMethod("POST")
        .withPath("/refresh")
        .withBody("client_secret=credential&grant_type=client_credentials&client_id=MY_CLIENTID");
    
    Map<String, Object> map = new TreeMap<>();
    
    map.put(EXPIRES_IN, "0987654321");
    map.put(TOKEN_TYPE, "bearer");
    map.put(ACCESS_TOKEN, AUTH_TOKEN);

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

    return expectedRequest;
  }

  public Configuration getConfiguration() {
    Configuration conf = new Configuration();

    // Configs for OAuth2
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, true);
    conf.set(OAUTH_CLIENT_ID_KEY, "MY_CLIENTID");

    conf.set(ACCESS_TOKEN_PROVIDER_KEY,
        ConfCredentialBasedAccessTokenProvider.class.getName());

    return conf;

  }

  @After
  public void stopMockWebHDFSServer() {
      mockWebHDFS.stop();
  }

  @After
  public void stopMockOAuthServer() {
    mockOAuthServer.stop();
  }
}

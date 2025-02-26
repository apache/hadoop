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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.helper;

import org.apache.hadoop.http.JettyUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * This class hides the implementation details of how to verify the structure of
 * JSON responses. Tests should only provide the path of the
 * {@link WebTarget}, the response from the resource and
 * the verifier Consumer to
 * {@link JsonCustomResourceTypeTestcase#verify(Consumer)}. An instance of
 * {@link JSONObject} will be passed to that consumer to be able to
 * verify the response.
 */
public class JsonCustomResourceTypeTestcase {
  private static final Logger LOG =
      LoggerFactory.getLogger(JsonCustomResourceTypeTestcase.class);

  private final WebTarget path;
  private final BufferedClientResponse response;
  private final JSONObject parsedResponse;

  public JsonCustomResourceTypeTestcase(WebTarget path,
      BufferedClientResponse response) throws JSONException {
    this.path = path;
    verifyStatus(response);
    this.response = response;
    String entity = response.getEntity(String.class);
    this.parsedResponse = new JSONObject(entity);
  }

  private void verifyStatus(BufferedClientResponse response) {
    String responseStr = response.getEntity(String.class);
    String exceptMessgae = String.format("HTTP status should be 200, " +
        "status info:{} response as string:{}", response.getStatusInfo(), responseStr);
    assertEquals(exceptMessgae, 200, response.getStatus());
  }

  public void verify(Consumer<JSONObject> verifier) {
    assertEquals(MediaType.APPLICATION_JSON_TYPE + ";" + JettyUtils.UTF_8,
        response.getType().toString());

    logResponse();

    String responseStr = response.getEntity(String.class);
    if (responseStr == null || responseStr.isEmpty()) {
      throw new IllegalStateException("Response is null or empty!");
    }
    verifier.accept(parsedResponse);
  }

  private void logResponse() {
    String responseStr = response.getEntity(String.class);
    LOG.info("Raw response from service URL {}: {}", path, responseStr);
    LOG.info("Parsed response from service URL {}: {}", path, parsedResponse);
  }
}
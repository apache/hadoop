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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Helper class the has constants and helper methods
 * used in WASB when integrating with a remote http cred
 * service. Currently, remote service will be used to generate
 * SAS keys.
 */
class WasbRemoteCallHelper {

  /**
   * Return code when the remote call is successful. {@value}
   */
  public static final int REMOTE_CALL_SUCCESS_CODE = 0;

  /**
   * Client instance to be used for making the remote call.
   */
  private HttpClient client = null;

  public WasbRemoteCallHelper() {
    this.client = HttpClientBuilder.create().build();
  }

  /**
   * Helper method to make remote HTTP Get request.
   * @param getRequest - HttpGet request object constructed by caller.
   * @return Http Response body returned as a string. The caller
   *  is expected to semantically understand the response.
   * @throws WasbRemoteCallException
   */
  public String makeRemoteGetRequest(HttpGet getRequest)
      throws WasbRemoteCallException {

    try {

      HttpResponse response = client.execute(getRequest);

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new WasbRemoteCallException(
            response.getStatusLine().toString());
      }

      BufferedReader rd = new BufferedReader(
          new InputStreamReader(response.getEntity().getContent(),
              StandardCharsets.UTF_8));
      StringBuilder responseBody = new StringBuilder();
      String responseLine = "";
      while ((responseLine = rd.readLine()) != null) {
        responseBody.append(responseLine);
      }
      rd.close();
      return responseBody.toString();

    } catch (ClientProtocolException clientProtocolEx) {
      throw new WasbRemoteCallException("Encountered ClientProtocolException"
          + " while making remote call", clientProtocolEx);
    } catch (IOException ioEx) {
      throw new WasbRemoteCallException("Encountered IOException while making"
          + " remote call", ioEx);
    }
  }
}
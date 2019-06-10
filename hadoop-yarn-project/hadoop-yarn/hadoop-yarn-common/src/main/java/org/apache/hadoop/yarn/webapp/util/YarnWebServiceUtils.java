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
package org.apache.hadoop.yarn.webapp.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource.Builder;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jettison.json.JSONObject;

import java.io.StringWriter;

/**
 * This class contains several utility function which could be used to generate
 * Restful calls to RM/NM/AHS.
 *
 */
public final class YarnWebServiceUtils {

  private YarnWebServiceUtils() {}

  /**
   * Utility function to get NodeInfo by calling RM WebService.
   * @param conf the configuration
   * @param nodeId the nodeId
   * @return a JSONObject which contains the NodeInfo
   * @throws ClientHandlerException if there is an error
   *         processing the response.
   * @throws UniformInterfaceException if the response status
   *         is 204 (No Content).
   */
  public static JSONObject getNodeInfoFromRMWebService(Configuration conf,
      String nodeId) throws ClientHandlerException,
      UniformInterfaceException {
    try {
      return WebAppUtils.execOnActiveRM(conf,
          YarnWebServiceUtils::getNodeInfoFromRM, nodeId);
    } catch (Exception e) {
      if (e instanceof ClientHandlerException) {
        throw ((ClientHandlerException) e);
      } else if (e instanceof UniformInterfaceException) {
        throw ((UniformInterfaceException) e);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private static JSONObject getNodeInfoFromRM(String webAppAddress,
      String nodeId) throws ClientHandlerException, UniformInterfaceException {
    Client webServiceClient = Client.create();
    ClientResponse response = null;
    try {
      Builder builder = webServiceClient.resource(webAppAddress)
          .path("ws").path("v1").path("cluster")
          .path("nodes").path(nodeId).accept(MediaType.APPLICATION_JSON);
      response = builder.get(ClientResponse.class);
      return response.getEntity(JSONObject.class);
    } finally {
      if (response != null) {
        response.close();
      }
      webServiceClient.destroy();
    }
  }

  @SuppressWarnings("rawtypes")
  public static String toJson(Object nsli, Class klass) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(nsli, sw);
    return sw.toString();
  }
}

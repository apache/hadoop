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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
   * @param nodeId the node
   * @return a JSONObject which contains the NodeInfo
   */
  public static JSONObject getNodeInfoFromRMWebService(Configuration conf,
      String nodeId) throws Exception {
    return WebAppUtils.execOnActiveRM(conf,
          YarnWebServiceUtils::getNodeInfoFromRM, nodeId);
  }

  private static JSONObject getNodeInfoFromRM(String webAppAddress,
      String nodeId) {
    Client webServiceClient = ClientBuilder.newClient();
    Response response = null;
    try {
      WebTarget target = webServiceClient.target(webAppAddress)
          .path("ws").path("v1").path("cluster")
          .path("nodes").path(nodeId);
      response = target.request(MediaType.APPLICATION_JSON).get(Response.class);
      return response.readEntity(JSONObject.class);
    } finally {
      if (response != null) {
        response.close();
      }
      webServiceClient.close();
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

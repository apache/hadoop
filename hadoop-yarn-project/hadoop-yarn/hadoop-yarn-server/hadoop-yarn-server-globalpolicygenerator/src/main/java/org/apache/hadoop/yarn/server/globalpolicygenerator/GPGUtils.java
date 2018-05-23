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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import static javax.servlet.http.HttpServletResponse.SC_OK;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * GPGUtils contains utility functions for the GPG.
 *
 */
public final class GPGUtils {

  // hide constructor
  private GPGUtils() {
  }

  /**
   * Performs an invocation of the the remote RMWebService.
   */
  public static <T> T invokeRMWebService(Configuration conf, String webAddr,
      String path, final Class<T> returnType) {
    Client client = Client.create();
    T obj = null;

    WebResource webResource = client.resource(webAddr);
    ClientResponse response = null;
    try {
      response = webResource.path("ws/v1/cluster").path(path)
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      if (response.getStatus() == SC_OK) {
        obj = response.getEntity(returnType);
      } else {
        throw new YarnRuntimeException(
            "Bad response from remote web service: " + response.getStatus());
      }
      return obj;
    } finally {
      if (response != null) {
        response.close();
      }
      client.destroy();
    }
  }

  /**
   * Creates a uniform weighting of 1.0 for each sub cluster.
   */
  public static Map<SubClusterIdInfo, Float> createUniformWeights(
      Set<SubClusterId> ids) {
    Map<SubClusterIdInfo, Float> weights =
        new HashMap<>();
    for(SubClusterId id : ids) {
      weights.put(new SubClusterIdInfo(id), 1.0f);
    }
    return weights;
  }

}

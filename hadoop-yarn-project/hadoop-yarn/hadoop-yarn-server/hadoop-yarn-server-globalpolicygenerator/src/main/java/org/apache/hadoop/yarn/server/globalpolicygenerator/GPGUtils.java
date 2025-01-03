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
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts.RM_WEB_SERVICE_PATH;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.HTTPS_PREFIX;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.HTTP_PREFIX;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GPGUtils contains utility functions for the GPG.
 */
public final class GPGUtils {

  private static final Logger LOG = LoggerFactory.getLogger(GPGUtils.class);

  // hide constructor
  private GPGUtils() {
  }

  /**
   * Performs an invocation of the remote RMWebService.
   *
   * @param <T> Generic T.
   * @param webAddr WebAddress.
   * @param path url path.
   * @param returnType return type.
   * @param selectParam query parameters.
   * @param conf configuration.
   * @return response entity.
   */
  public static <T> T invokeRMWebService(String webAddr, String path, final Class<T> returnType,
      Configuration conf, String selectParam) {
    Client client = createJerseyClient(conf);
    T obj;

    // webAddr stores the form of host:port in subClusterInfo
    InetSocketAddress socketAddress = NetUtils
        .getConnectAddress(NetUtils.createSocketAddr(webAddr));
    String scheme = YarnConfiguration.useHttps(conf) ? HTTPS_PREFIX : HTTP_PREFIX;
    String webAddress = scheme + socketAddress.getHostName() + ":" + socketAddress.getPort();
    WebTarget webResource = client.target(webAddress);

    if (selectParam != null) {
      webResource = webResource.queryParam(RMWSConsts.DESELECTS, selectParam);
    }

    Response response = null;
    try {
      response = webResource.path(RM_WEB_SERVICE_PATH).path(path)
          .request(MediaType.APPLICATION_XML).get(Response.class);
      if (response.getStatus() == SC_OK) {
        obj = response.readEntity(returnType);
      } else {
        throw new YarnRuntimeException(
            "Bad response from remote web service: " + response.getStatus());
      }
    } finally {
      if (response != null) {
        response.close();
      }
      client.close();
    }
    return obj;
  }

  /**
   * Performs an invocation of the remote RMWebService.
   *
   * @param <T> Generic T.
   * @param webAddr WebAddress.
   * @param path url path.
   * @param returnType return type.
   * @param config configuration.
   * @return response entity.
   */
  public static <T> T invokeRMWebService(String webAddr,
      String path, final Class<T> returnType, Configuration config) {
    return invokeRMWebService(webAddr, path, returnType, config, null);
  }

  /**
   * Creates a uniform weighting of 1.0 for each sub cluster.
   *
   * @param ids subClusterId set
   * @return weight of subCluster.
   */
  public static Map<SubClusterIdInfo, Float> createUniformWeights(
      Set<SubClusterId> ids) {
    Map<SubClusterIdInfo, Float> weights = new HashMap<>();
    for(SubClusterId id : ids) {
      weights.put(new SubClusterIdInfo(id), 1.0f);
    }
    return weights;
  }

  /**
   * Create JerseyClient based on configuration file.
   * We will set the timeout when creating JerseyClient.
   *
   * @param conf Configuration.
   * @return Jersey Client
   */
  public static Client createJerseyClient(Configuration conf) {
    Client client = ClientBuilder.newClient();
    int connectTimeOut = (int) conf.getTimeDuration(YarnConfiguration.GPG_WEBAPP_CONNECT_TIMEOUT,
        YarnConfiguration.DEFAULT_GPG_WEBAPP_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeOut);
    int readTimeout = (int) conf.getTimeDuration(YarnConfiguration.GPG_WEBAPP_READ_TIMEOUT,
        YarnConfiguration.DEFAULT_GPG_WEBAPP_READ_TIMEOUT, TimeUnit.MILLISECONDS);
    client.property(ClientProperties.READ_TIMEOUT, readTimeout);
    return client;
  }
}

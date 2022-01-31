/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

/**
 * Constants for {@code StateStoreWebServiceProtocol}.
 */
public class HttpProxyFederationStateStoreConsts {
  // Query Parameters
  public static final String QUERY_SC_FILTER = "filterInactiveSubClusters";

  // URL Parameters
  public static final String PARAM_SCID = "subcluster";
  public static final String PARAM_APPID = "appid";
  public static final String PARAM_QUEUE = "queue";

  // Paths
  public static final String ROOT = "/ws/v1/statestore";

  public static final String PATH_SUBCLUSTERS = "/subclusters";
  public static final String PATH_SUBCLUSTERS_SCID =
      "/subclusters/{" + PARAM_SCID + "}";

  public static final String PATH_POLICY = "/policy";
  public static final String PATH_POLICY_QUEUE =
      "/policy/{" + PARAM_QUEUE + "}";

  public static final String PATH_VIP_HEARTBEAT = "/heartbeat";

  public static final String PATH_APP_HOME = "/apphome";
  public static final String PATH_APP_HOME_APPID =
      "/apphome/{" + PARAM_APPID + "}";

  public static final String PATH_REGISTER = "/subcluster/register";
  public static final String PATH_DEREGISTER = "/subcluster/deregister";
  public static final String PATH_HEARTBEAT = "/subcluster/heartbeat";
}


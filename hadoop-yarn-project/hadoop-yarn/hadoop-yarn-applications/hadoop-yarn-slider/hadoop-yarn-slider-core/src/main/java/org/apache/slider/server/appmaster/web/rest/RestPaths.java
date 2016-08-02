/*
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

package org.apache.slider.server.appmaster.web.rest;

/**
 * Paths in the REST App
 */
public class RestPaths {

  public static final String WS_CONTEXT = "ws";
  public static final String AGENT_WS_CONTEXT = "ws";

  /**
   * Root path for the web services context: {@value}
   */
  public static final String WS_CONTEXT_ROOT = "/" + WS_CONTEXT;

  /**
   * agent content root: {@value}
   */
  public static final String WS_AGENT_CONTEXT_ROOT = "/" + AGENT_WS_CONTEXT;
  public static final String V1_SLIDER = "/v1/slider";
  public static final String SLIDER_CONTEXT_ROOT = WS_CONTEXT_ROOT + V1_SLIDER;
  public static final String RELATIVE_API = WS_CONTEXT + V1_SLIDER;
  public static final String SLIDER_AGENT_CONTEXT_ROOT = WS_AGENT_CONTEXT_ROOT + V1_SLIDER;
  public static final String MANAGEMENT = "mgmt";
  public static final String SLIDER_SUBPATH_MANAGEMENT = "/" + MANAGEMENT;
  public static final String SLIDER_SUBPATH_AGENTS = "/agents";
  public static final String SLIDER_SUBPATH_PUBLISHER = "/publisher";


  /**
   * management path: {@value}
   */
  public static final String SLIDER_PATH_MANAGEMENT = SLIDER_CONTEXT_ROOT
                                      + SLIDER_SUBPATH_MANAGEMENT;

  public static final String RELATIVE_PATH_MANAGEMENT = RELATIVE_API
                                      + SLIDER_SUBPATH_MANAGEMENT;

  /**
   * Agents: {@value}
   */
  public static final String SLIDER_PATH_AGENTS = SLIDER_AGENT_CONTEXT_ROOT
                                      + SLIDER_SUBPATH_AGENTS;
  
  /**
   * Publisher: {@value}
   */
  public static final String SLIDER_PATH_PUBLISHER = SLIDER_CONTEXT_ROOT
                                      + SLIDER_SUBPATH_PUBLISHER;

  public static final String RELATIVE_PATH_PUBLISHER = RELATIVE_API
                                      + SLIDER_SUBPATH_PUBLISHER;

  /**
   * Registry subpath: {@value} 
   */
  public static final String SLIDER_SUBPATH_REGISTRY = "/registry";

  /**
   * Registry: {@value}
   */
  public static final String SLIDER_PATH_REGISTRY = SLIDER_CONTEXT_ROOT
                                                    + SLIDER_SUBPATH_REGISTRY;
  public static final String RELATIVE_PATH_REGISTRY = RELATIVE_API
                                                    + SLIDER_SUBPATH_REGISTRY;


  /**
   * The regular expressions used to define valid configuration names/url path
   * fragments: {@value}
   */
  public static final String PUBLISHED_CONFIGURATION_REGEXP
      = "[a-z0-9][a-z0-9_\\+-]*";

  public static final String PUBLISHED_CONFIGURATION_SET_REGEXP
      = "[a-z0-9][a-z0-9_.\\+-]*";

  public static final String SLIDER_CONFIGSET = "slider";
  public static final String SLIDER_EXPORTS = "exports";

  public static final String SLIDER_CLASSPATH = "classpath";

  /**
   * Codahale Metrics - base path: {@value}
   */

  public static final String SYSTEM = "/system";


  /**
   * Codahale Metrics - health: {@value}
   */
  public static final String SYSTEM_HEALTHCHECK = SYSTEM + "/health";
  /**
   * Codahale Metrics - metrics: {@value}
   */
  public static final String SYSTEM_METRICS = SYSTEM + "/metrics";
  /**
   * Codahale Metrics - metrics as JSON: {@value}
   */
  public static final String SYSTEM_METRICS_JSON = SYSTEM_METRICS + "?format=json";
  /**
   * Codahale Metrics - ping: {@value}
   */
  public static final String SYSTEM_PING = SYSTEM + "/ping";
  /**
   * Codahale Metrics - thread dump: {@value}
   */
  public static final String SYSTEM_THREADS = SYSTEM + "/threads";

  /**
   * application subpath
   */
  public static final String SLIDER_SUBPATH_APPLICATION = "/application";
  
  /**
   * management path: {@value}
   */
  public static final String SLIDER_PATH_APPLICATION =
      SLIDER_CONTEXT_ROOT + SLIDER_SUBPATH_APPLICATION;


  public static final String APPLICATION_WADL = "/application.wadl";
  public static final String LIVE = "/live";
  public static final String LIVE_RESOURCES = "/live/resources";
  public static final String LIVE_CONTAINERS = "/live/containers";
  public static final String LIVE_COMPONENTS = "/live/components";
  public static final String LIVE_NODES = "/live/nodes";
  public static final String LIVE_LIVENESS = "/live/liveness";
  public static final String LIVE_STATISTICS = "/live/statistics";
  public static final String MODEL = "/model";
  public static final String MODEL_DESIRED = MODEL +"/desired";
  public static final String MODEL_DESIRED_APPCONF = MODEL_DESIRED +"/appconf";
  public static final String MODEL_DESIRED_RESOURCES = MODEL_DESIRED +"/resources";
  public static final String MODEL_RESOLVED = "/model/resolved";
  public static final String MODEL_RESOLVED_APPCONF = MODEL_RESOLVED +"/appconf";
  public static final String MODEL_RESOLVED_RESOURCES = MODEL_RESOLVED +"/resources";
  public static final String MODEL_INTERNAL = "/model/internal";

  public static final String ACTION = "/action";
  public static final String ACTION_PING = ACTION + "/ping";
  public static final String ACTION_STOP = ACTION + "/stop";

  /**
   * Path to a role
   * @param name role name
   * @return a path to it
   */
  public String pathToRole(String name) {

    // ws/v1/slider/application/live/components/$name
    return SLIDER_PATH_APPLICATION + LIVE_COMPONENTS + "/" + name;
  }
}

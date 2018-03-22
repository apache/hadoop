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

package org.apache.hadoop.yarn.service.registry;

/**
 * These are constants unique to the Slider AM
 */
public class CustomRegistryConstants {

  public static final String MANAGEMENT_REST_API =
      "classpath:org.apache.slider.management";
  
  public static final String REGISTRY_REST_API =
      "classpath:org.apache.slider.registry";
  
  public static final String PUBLISHER_REST_API =
      "classpath:org.apache.slider.publisher";

  public static final String PUBLISHER_CONFIGURATIONS_API =
      "classpath:org.apache.slider.publisher.configurations";

  public static final String PUBLISHER_EXPORTS_API =
      "classpath:org.apache.slider.publisher.exports";

  public static final String PUBLISHER_DOCUMENTS_API =
      "classpath:org.apache.slider.publisher.documents";

  public static final String AGENT_SECURE_REST_API =
      "classpath:org.apache.slider.agents.secure";

  public static final String AGENT_ONEWAY_REST_API =
      "classpath:org.apache.slider.agents.oneway";

  public static final String AM_IPC_PROTOCOL =
      "classpath:org.apache.slider.appmaster.ipc";

  public static final String AM_REST_BASE =
      "classpath:org.apache.slider.client.rest";

  public static final String WEB_UI = "http://";
}

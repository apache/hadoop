/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.conf;

import javax.ws.rs.core.MediaType;

public interface RestApiConstants {

  // Rest endpoints
  String CONTEXT_ROOT = "/v1";
  String VERSION = "/services/version";
  String SERVICE_ROOT_PATH = "/services";
  String SERVICE_PATH = "/services/{service_name}";
  String COMPONENT_PATH = "/services/{service_name}/components/{component_name}";

  String COMP_INSTANCE_PATH = SERVICE_PATH +
      "/component-instances/{component_instance_name}";
  String COMP_INSTANCE_LONG_PATH = COMPONENT_PATH +
      "/component-instances/{component_instance_name}";
  String COMP_INSTANCES = "component-instances";
  String COMP_INSTANCES_PATH = SERVICE_PATH + "/" + COMP_INSTANCES;
  String COMPONENTS = "components";
  String COMPONENTS_PATH = SERVICE_PATH + "/" + COMPONENTS;

  String SERVICE_NAME = "service_name";
  String COMPONENT_NAME = "component_name";
  String COMP_INSTANCE_NAME = "component_instance_name";

  String PARAM_COMP_NAME = "componentName";
  String PARAM_VERSION = "version";
  String PARAM_CONTAINER_STATE = "containerState";

  String MEDIA_TYPE_JSON_UTF8 = MediaType.APPLICATION_JSON + ";charset=utf-8";

  Long DEFAULT_UNLIMITED_LIFETIME = -1l;

  Integer ERROR_CODE_APP_DOES_NOT_EXIST = 404001;
  Integer ERROR_CODE_APP_IS_NOT_RUNNING = 404002;
  Integer ERROR_CODE_APP_SUBMITTED_BUT_NOT_RUNNING_YET = 404003;
  Integer ERROR_CODE_APP_NAME_INVALID = 404004;
}

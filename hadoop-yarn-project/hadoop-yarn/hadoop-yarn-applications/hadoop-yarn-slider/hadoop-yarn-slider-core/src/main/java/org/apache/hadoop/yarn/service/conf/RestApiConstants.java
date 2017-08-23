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

public interface RestApiConstants {

  // Rest endpoints
  String CONTEXT_ROOT = "/services/v1";
  String VERSION = "/version";
  String APP_ROOT_PATH = "/applications";
  String APP_PATH = "/applications/{app_name}";
  String COMPONENT_PATH = "/applications/{app_name}/components/{component_name}";

  // Query param
  String APP_NAME = "app_name";
  String COMPONENT_NAME = "component_name";

  String DEFAULT_COMPONENT_NAME = "default";

  String PROPERTY_REST_SERVICE_HOST = "REST_SERVICE_HOST";
  String PROPERTY_REST_SERVICE_PORT = "REST_SERVICE_PORT";
  Long DEFAULT_UNLIMITED_LIFETIME = -1l;

  Integer ERROR_CODE_APP_DOES_NOT_EXIST = 404001;
  Integer ERROR_CODE_APP_IS_NOT_RUNNING = 404002;
  Integer ERROR_CODE_APP_SUBMITTED_BUT_NOT_RUNNING_YET = 404003;
  Integer ERROR_CODE_APP_NAME_INVALID = 404004;
}

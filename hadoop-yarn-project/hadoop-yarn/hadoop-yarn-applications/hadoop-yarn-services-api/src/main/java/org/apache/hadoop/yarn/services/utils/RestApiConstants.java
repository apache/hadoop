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

package org.apache.hadoop.yarn.services.utils;

public interface RestApiConstants {
  String CONTEXT_ROOT = "/services/v1";
  String APPLICATIONS_API_RESOURCE_PATH = "/applications";
  String CONTAINERS_API_RESOURCE_PATH = "/containers";
  String SLIDER_APPMASTER_COMPONENT_NAME = "slider-appmaster";
  String SLIDER_CONFIG_SCHEMA = "http://example.org/specification/v2.0.0";
  String METAINFO_SCHEMA_VERSION = "2.1";
  String COMPONENT_TYPE_YARN_DOCKER = "yarn_docker";

  String DEFAULT_START_CMD = "/bootstrap/privileged-centos6-sshd";
  String DEFAULT_COMPONENT_NAME = "DEFAULT";
  String DEFAULT_IMAGE = "centos:centos6";
  String DEFAULT_NETWORK = "bridge";
  String DEFAULT_COMMAND_PATH = "/usr/bin/docker";
  String DEFAULT_USE_NETWORK_SCRIPT = "yes";

  String PLACEHOLDER_APP_NAME = "${APP_NAME}";
  String PLACEHOLDER_APP_COMPONENT_NAME = "${APP_COMPONENT_NAME}";
  String PLACEHOLDER_COMPONENT_ID = "${COMPONENT_ID}";

  String PROPERTY_REST_SERVICE_HOST = "REST_SERVICE_HOST";
  String PROPERTY_REST_SERVICE_PORT = "REST_SERVICE_PORT";
  String PROPERTY_APP_LIFETIME = "docker.lifetime";
  String PROPERTY_APP_RUNAS_USER = "APP_RUNAS_USER";
  Long DEFAULT_UNLIMITED_LIFETIME = -1l;

  Integer HTTP_STATUS_CODE_ACCEPTED = 202;
  String ARTIFACT_TYPE_SLIDER_ZIP = "slider-zip";

  Integer GET_APPLICATIONS_THREAD_POOL_SIZE = 200;

  String PROPERTY_PYTHON_PATH = "python.path";
  String PROPERTY_DNS_DEPENDENCY = "site.global.dns.dependency";

  String COMMAND_ORDER_SUFFIX_START = "-START";
  String COMMAND_ORDER_SUFFIX_STARTED = "-STARTED";
  String EXPORT_GROUP_NAME = "QuickLinks";

  Integer ERROR_CODE_APP_DOES_NOT_EXIST = 404001;
  Integer ERROR_CODE_APP_IS_NOT_RUNNING = 404002;
  Integer ERROR_CODE_APP_SUBMITTED_BUT_NOT_RUNNING_YET = 404003;
  Integer ERROR_CODE_APP_NAME_INVALID = 404004;

}

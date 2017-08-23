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

package org.apache.hadoop.yarn.service.client.params;

/**
 * Actions.
 * Only some of these are supported by specific Slider Services; they
 * are listed here to ensure the names are consistent
 */
public interface SliderActions {
  String ACTION_BUILD = "build";
  String ACTION_CLIENT = "client";
  String ACTION_CREATE = "create";
  String ACTION_DEPENDENCY = "dependency";
  String ACTION_UPDATE = "update";
  String ACTION_UPGRADE = "upgrade";
  String ACTION_DESTROY = "destroy";
  String ACTION_EXISTS = "exists";
  String ACTION_FLEX = "flex";
  String ACTION_STOP = "stop";
  String ACTION_HELP = "help";
  String ACTION_INSTALL_KEYTAB = "install-keytab";
  String ACTION_KDIAG = "kdiag";
  String ACTION_KEYTAB = "keytab";
  String ACTION_LIST = "list";

  String ACTION_REGISTRY = "registry";
  String ACTION_RESOLVE = "resolve";
  String ACTION_RESOURCE = "resource";
  String ACTION_STATUS = "status";
  String ACTION_START = "start";
  String ACTION_TOKENS = "tokens";

  String DESCRIBE_ACTION_BUILD =
    "Build a service specification, but do not start it";
  String DESCRIBE_ACTION_CREATE =
      "Build and start a service, it's equivalent to first invoke build and then start";
  String DESCRIBE_ACTION_DEPENDENCY =
      "Yarn service framework dependency (libraries) management";
  String DESCRIBE_ACTION_UPDATE =
      "Update template for service";
  String DESCRIBE_ACTION_UPGRADE =
      "Rolling upgrade/downgrade the component/containerto a newer/previous version";
  String DESCRIBE_ACTION_DESTROY =
        "Destroy a stopped service, service must be stopped first before destroying.";
  String DESCRIBE_ACTION_EXISTS =
            "Probe for an application running";
  String DESCRIBE_ACTION_FLEX = "Flex a service's component by increasing or decreasing the number of containers.";
  String DESCRIBE_ACTION_FREEZE =
              "Stop a running service";
  String DESCRIBE_ACTION_KDIAG = "Diagnose Kerberos problems";
  String DESCRIBE_ACTION_HELP = "Print help information";
  String DESCRIBE_ACTION_LIST =
                  "List running services";
  String DESCRIBE_ACTION_REGISTRY =
                      "Query the registry of a service";
  String DESCRIBE_ACTION_STATUS =
                      "Get the status of a service";
  String DESCRIBE_ACTION_THAW =
                        "Start a service with pre-built specification or a previously stopped service";
  String DESCRIBE_ACTION_CLIENT = "Install the application client in the specified directory or obtain a client keystore or truststore";
  String DESCRIBE_ACTION_KEYTAB = "Manage a Kerberos keytab file (install, delete, list) in the sub-folder 'keytabs' of the user's Slider base directory";
  String DESCRIBE_ACTION_RESOURCE = "Manage a file (install, delete, list) in the 'resources' sub-folder of the user's Slider base directory";

}


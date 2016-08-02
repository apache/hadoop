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

package org.apache.slider.core.exceptions;

public interface ErrorStrings {
  String E_UNSTABLE_CLUSTER = "Unstable Application Instance :";
  String E_CLUSTER_RUNNING = "Application Instance running";
  String E_ALREADY_EXISTS = "already exists";
  String PRINTF_E_INSTANCE_ALREADY_EXISTS = "Application Instance \"%s\" already exists and is defined in %s";
  String PRINTF_E_INSTANCE_DIR_ALREADY_EXISTS = "Application Instance dir already exists: %s";
  String E_MISSING_PATH = "Missing path ";
  String E_INCOMPLETE_CLUSTER_SPEC =
    "Cluster specification is marked as incomplete: ";
  String E_UNKNOWN_INSTANCE = "Unknown application instance ";
  String E_DESTROY_CREATE_RACE_CONDITION =
      "created while it was being destroyed";
  String E_UNKNOWN_ROLE = "Unknown role ";
  /**
   * ERROR Strings
   */
  String ERROR_NO_ACTION = "No action specified";
  String ERROR_UNKNOWN_ACTION = "Unknown command: ";
  String ERROR_NOT_ENOUGH_ARGUMENTS =
    "Not enough arguments for action: ";
  String ERROR_PARSE_FAILURE =
      "Failed to parse ";
  /**
   * All the remaining values after argument processing
   */
  String ERROR_TOO_MANY_ARGUMENTS =
    "Too many arguments";
  String ERROR_DUPLICATE_ENTRY = "Duplicate entry for ";
  String E_APPLICATION_NOT_RUNNING = "Application not running";
  String E_FINISHED_APPLICATION = E_APPLICATION_NOT_RUNNING + ": %s state=%s ";
  String E_NO_IMAGE_OR_HOME_DIR_SPECIFIED =
    "Neither an image path nor binary home directory were specified";
  String E_BOTH_IMAGE_AND_HOME_DIR_SPECIFIED =
    "Both application image path and home dir have been provided";
  String E_CONFIGURATION_DIRECTORY_NOT_FOUND =
    "Configuration directory \"%s\" not found";
}

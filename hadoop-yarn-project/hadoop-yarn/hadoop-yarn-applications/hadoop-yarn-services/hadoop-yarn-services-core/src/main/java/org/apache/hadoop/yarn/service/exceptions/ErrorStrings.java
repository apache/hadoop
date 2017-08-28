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

package org.apache.hadoop.yarn.service.exceptions;

public interface ErrorStrings {

  String PRINTF_E_INSTANCE_ALREADY_EXISTS = "Service Instance \"%s\" already exists and is defined in %s";
  String PRINTF_E_INSTANCE_DIR_ALREADY_EXISTS = "Service Instance dir already exists: %s";

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

}

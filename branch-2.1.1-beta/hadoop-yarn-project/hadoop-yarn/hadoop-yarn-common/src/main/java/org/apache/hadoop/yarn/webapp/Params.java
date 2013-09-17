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

package org.apache.hadoop.yarn.webapp;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Public static constants for webapp parameters. Do NOT put any
 * private or application specific constants here as they're part of
 * the API for users of the controllers and views.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public interface Params {
  static final String TITLE = "title";
  static final String TITLE_LINK = "title.href";
  static final String USER = "user";
  static final String ERROR_DETAILS = "error.details";
}
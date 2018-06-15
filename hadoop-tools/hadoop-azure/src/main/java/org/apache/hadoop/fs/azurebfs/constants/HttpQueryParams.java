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
package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Responsible to keep all Http Query params here
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class HttpQueryParams {
  public static final String QUERY_PARAM_RESOURCE = "resource";
  public static final String QUERY_PARAM_DIRECTORY = "directory";
  public static final String QUERY_PARAM_CONTINUATION = "continuation";
  public static final String QUERY_PARAM_RECURSIVE = "recursive";
  public static final String QUERY_PARAM_MAXRESULTS = "maxResults";
  public static final String QUERY_PARAM_ACTION = "action";
  public static final String QUERY_PARAM_POSITION = "position";
  public static final String QUERY_PARAM_TIMEOUT = "timeout";
  public static final String QUERY_PARAM_RETAIN_UNCOMMITTED_DATA = "retainUncommittedData";

  private HttpQueryParams() {}
}

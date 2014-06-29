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

package org.apache.hadoop.service.launcher;

/**
 * Standard launcher arguments. These are all from
 * the GenericOptionsParser -simply extracted to constants
 */
public interface LauncherArguments {
  /**
   * Name of the configuration argument on the CLI: {@value} 
   */
  String ARG_CONF = "conf";

  String ARG_FS = "fs";
  
  String ARG_RM = "jt";

  String ARG_DEF = "D";

  String ARG_LIBJARS = "libjars";

  String ARG_PATHS = "paths";

  String ARG_FILES = "files";

  String ARG_ARCHIVES = "archives";

  String ARG_TOKEN_CACHE_FILE = "tokenCacheFile";

  String E_PARSE_FAILED = "Failed to parse:";
}

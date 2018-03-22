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
 * the {@code GenericOptionsParser}, simply extracted to constants.
 */
public interface LauncherArguments {
  /**
   * Name of the configuration argument on the CLI.
   * Value: {@value}
   */
  String ARG_CONF = "conf";
  String ARG_CONF_SHORT = "conf";

  /**
   * prefixed version of {@link #ARG_CONF}.
   * Value: {@value}
   */
  String ARG_CONF_PREFIXED = "--" + ARG_CONF;

  /**
   * Name of a configuration class which is loaded before any
   * attempt is made to load the class.
   * <p>
   * Value: {@value}
   */
  String ARG_CONFCLASS = "hadoopconf";
  String ARG_CONFCLASS_SHORT = "hadoopconf";

  /**
   * Prefixed version of {@link #ARG_CONFCLASS}.
   * Value: {@value}
   */
  String ARG_CONFCLASS_PREFIXED = "--" + ARG_CONFCLASS;

  /**
   * Error string on a parse failure.
   * Value: {@value}
   */
  String E_PARSE_FAILED = "Failed to parse: ";
}

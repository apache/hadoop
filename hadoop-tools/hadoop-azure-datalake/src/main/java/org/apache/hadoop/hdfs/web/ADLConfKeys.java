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
 *
 */

package org.apache.hadoop.hdfs.web;

/**
 * Constants.
 */
public final class ADLConfKeys {
  public static final String
      ADL_FEATURE_CONCURRENT_READ_AHEAD_MAX_CONCURRENT_CONN =
      "adl.feature.override.readahead.max.concurrent.connection";
  public static final int
      ADL_FEATURE_CONCURRENT_READ_AHEAD_MAX_CONCURRENT_CONN_DEFAULT = 2;
  public static final String ADL_WEBSDK_VERSION_KEY = "ADLFeatureSet";
  static final String ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER =
      "adl.debug.override.localuserasfileowner";
  static final boolean ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT = false;
  static final String ADL_FEATURE_REDIRECT_OFF =
      "adl.feature.override.redirection.off";
  static final boolean ADL_FEATURE_REDIRECT_OFF_DEFAULT = true;
  static final String ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED =
      "adl.feature.override.getblocklocation.locally.bundled";
  static final boolean ADL_FEATURE_GET_BLOCK_LOCATION_LOCALLY_BUNDLED_DEFAULT
      = true;
  static final String ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD =
      "adl.feature.override.readahead";
  static final boolean ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_DEFAULT =
      true;
  static final String ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE =
      "adl.feature.override.readahead.max.buffersize";

  static final int KB = 1024;
  static final int MB = KB * KB;
  static final int DEFAULT_BLOCK_SIZE = 4 * MB;
  static final int DEFAULT_EXTENT_SIZE = 256 * MB;
  static final int DEFAULT_TIMEOUT_IN_SECONDS = 120;
  static final int
      ADL_FEATURE_CONCURRENT_READ_WITH_READ_AHEAD_BUFFER_SIZE_DEFAULT =
      8 * MB;

  private ADLConfKeys() {
  }

}

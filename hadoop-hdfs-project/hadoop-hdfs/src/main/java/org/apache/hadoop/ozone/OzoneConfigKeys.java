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

package org.apache.hadoop.ozone;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This class contains constants for configuration keys used in Ozone.
 */
@InterfaceAudience.Private
public final class OzoneConfigKeys {
  public static final String DFS_CONTAINER_IPC_PORT =
      "dfs.container.ipc";
  public static final int DFS_CONTAINER_IPC_PORT_DEFAULT =  50011;
  public static final String OZONE_LOCALSTORAGE_ROOT =
      "ozone.localstorage.root";
  public static final String OZONE_LOCALSTORAGE_ROOT_DEFAULT = "/tmp/ozone";
  public static final String OZONE_ENABLED =
      "ozone.enabled";
  public static final boolean OZONE_ENABLED_DEFAULT = false;
  public static final String OZONE_HANDLER_TYPE_KEY =
      "ozone.handler.type";
  public static final String OZONE_HANDLER_TYPE_DEFAULT = "distributed";
  public static final String DFS_CONTAINER_LOCATION_RPC_ADDRESS_KEY =
      "dfs.container.location.rpc-address";
  public static final int DFS_CONTAINER_LOCATION_RPC_DEFAULT_PORT = 50200;
  public static final String DFS_CONTAINER_LOCATION_RPC_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_CONTAINER_LOCATION_RPC_DEFAULT_PORT;
  public static final String DFS_CONTAINER_LOCATION_RPC_BIND_HOST_KEY =
      "dfs.container.rpc-bind-host";
  public static final String DFS_CONTAINER_LOCATION_HANDLER_COUNT_KEY =
      "dfs.container.handler.count";
  public static final int DFS_CONTAINER_HANDLER_COUNT_DEFAULT = 10;
  public static final String OZONE_TRACE_ENABLED_KEY =
      "ozone.trace.enabled";
  public static final boolean OZONE_TRACE_ENABLED_DEFAULT = false;

  public static final String OZONE_METADATA_DIRS =
      "ozone.metadata.dirs";

  public static final String OZONE_KEY_CACHE = "ozone.key.cache.size";
  public static final int OZONE_KEY_CACHE_DEFAULT = 1024;



  /**
   * There is no need to instantiate this class.
   */
  private OzoneConfigKeys() {
  }
}

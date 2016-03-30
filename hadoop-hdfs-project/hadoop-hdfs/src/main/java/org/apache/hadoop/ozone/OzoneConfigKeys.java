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
  public static final String DFS_OZONE_CONTAINER_IPC_PORT =
      "dfs.ozone.container.ipc";
  public static final int DFS_OZONE_CONTAINER_IPC_PORT_DEFAULT =  50011;
  public static final String DFS_STORAGE_LOCAL_ROOT =
      "dfs.ozone.localstorage.root";
  public static final String DFS_STORAGE_LOCAL_ROOT_DEFAULT = "/tmp/ozone";
  public static final String DFS_OBJECTSTORE_ENABLED_KEY =
      "dfs.objectstore.enabled";
  public static final boolean DFS_OBJECTSTORE_ENABLED_DEFAULT = false;
  public static final String DFS_STORAGE_HANDLER_TYPE_KEY =
      "dfs.storage.handler.type";
  public static final String DFS_STORAGE_HANDLER_TYPE_DEFAULT = "distributed";
  public static final String DFS_STORAGE_RPC_ADDRESS_KEY =
      "dfs.storage.rpc-address";
  public static final int DFS_STORAGE_RPC_DEFAULT_PORT = 50200;
  public static final String DFS_STORAGE_RPC_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_STORAGE_RPC_DEFAULT_PORT;
  public static final String DFS_STORAGE_RPC_BIND_HOST_KEY =
      "dfs.storage.rpc-bind-host";
  public static final String DFS_STORAGE_HANDLER_COUNT_KEY =
      "dfs.storage.handler.count";
  public static final int DFS_STORAGE_HANDLER_COUNT_DEFAULT = 10;
  public static final String DFS_OBJECTSTORE_TRACE_ENABLED_KEY =
      "dfs.objectstore.trace.enabled";
  public static final boolean DFS_OBJECTSTORE_TRACE_ENABLED_DEFAULT = false;

  public static final String DFS_OZONE_METADATA_DIRS =
      "dfs.ozone.metadata.dirs";


  /**
   * There is no need to instantiate this class.
   */
  private OzoneConfigKeys() {
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
  public static final String DFS_STORAGE_LOCAL_ROOT =
      "dfs.ozone.localstorage.root";
  public static final String DFS_STORAGE_LOCAL_ROOT_DEFAULT = "/tmp/ozone";

  /**
   * There is no need to instantiate this class.
   */
  private OzoneConfigKeys() {
  }
}

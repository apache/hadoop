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

package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface for bulk deletion.
 * Filesystems which support bulk deletion should implement this interface
 * and MUST also declare their support in the path capability
 * {@link CommonPathCapabilities#BULK_DELETE}.
 * Exporting the interface does not guarantee that the operation is supported;
 * returning a {@link BulkDelete} object from the call {@link #createBulkDelete(Path)}
 * is.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BulkDeleteSource {

  /**
   * Create a bulk delete operation.
   * There is no network IO at this point, simply the creation of
   * a bulk delete object.
   * A path must be supplied to assist in link resolution.
   * @param path path to delete under.
   * @return the bulk delete.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException path not valid.
   * @throws IOException problems resolving paths
   */
  BulkDelete createBulkDelete(Path path)
      throws UnsupportedOperationException, IllegalArgumentException, IOException;

}

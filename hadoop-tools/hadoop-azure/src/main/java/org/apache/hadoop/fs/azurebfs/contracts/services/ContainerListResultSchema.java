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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.ArrayList;
import java.util.List;

/**
 * The ListResultSchema model for Blob Endpoint Container Listing.
 *
 * <p>
 * Represents the parsed response of the Azure Blob Storage
 * List Containers REST API.
 * </p>
 */
public class ContainerListResultSchema implements ListResultSchema {

  /** List of containers returned by the listing operation. */
  private List<ContainerListEntrySchema> containers;

  /** Continuation token for the next page of results. */
  private String nextMarker;

  public ContainerListResultSchema() {
    this.containers = new ArrayList<>();
    this.nextMarker = null;
  }

  /**
   * Returns the list of containers returned by the listing operation.
   *
   * @return list of container entries
   */
  @Override
  public List<ContainerListEntrySchema> paths() {
    return containers;
  }

  /**
   * Sets the container entries returned by the listing operation.
   *
   * @param paths list of container entries
   * @return the ListResultSchema object itself
   */
  @Override
  @SuppressWarnings("unchecked")
  public ListResultSchema withPaths(
      final List<? extends ListResultEntrySchema> paths) {
    this.containers = (List<ContainerListEntrySchema>) paths;
    return this;
  }

  /**
   * Adds a container entry to the result.
   *
   * @param entry container entry
   */
  public void addContainerEntry(
      final ContainerListEntrySchema entry) {
    this.containers.add(entry);
  }

  /**
   * Returns the continuation token (NextMarker).
   *
   * @return continuation token
   */
  public String getNextMarker() {
    return nextMarker;
  }

  /**
   * Sets the continuation token (NextMarker).
   *
   * @param nextMarker continuation token
   */
  public void setNextMarker(final String nextMarker) {
    this.nextMarker = nextMarker;
  }
}

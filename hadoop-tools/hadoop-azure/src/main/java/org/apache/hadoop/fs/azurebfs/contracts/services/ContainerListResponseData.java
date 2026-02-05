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
 * Response model for the Azure Blob Storage List Containers REST API.
 *
 * Holds the list of containers returned by the service along with
 * pagination metadata such as continuation token and markers.
 */
public class ContainerListResponseData {

  /** List of containers returned by the listing operation. */
  private final List<ContainerListEntrySchema> containers =
      new ArrayList<>();

  /** Continuation token for fetching the next page of results. */
  private String continuationToken;

  /** Prefix used for filtering containers. */
  private String prefix;

  /** Marker indicating the start point of the listing. */
  private String marker;

  /** Maximum number of results requested per page. */
  private Integer maxResults;

  /**
   * Adds a container entry to the response.
   *
   * @param container container entry to add
   */
  public void addContainer(ContainerListEntrySchema container) {
    containers.add(container);
  }

  /**
   * Returns the list of containers in the response.
   *
   * @return list of container entries
   */
  public List<ContainerListEntrySchema> getContainers() {
    return containers;
  }

  /**
   * Returns the continuation token for the next page of results.
   *
   * @return continuation token, or {@code null} if no more results
   */
  public String getContinuationToken() {
    return continuationToken;
  }

  /**
   * Returns the prefix used for the listing operation.
   *
   * @return container name prefix
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * Returns the marker used for the listing operation.
   *
   * @return listing marker
   */
  public String getMarker() {
    return marker;
  }

  /**
   * Returns the maximum number of results requested.
   *
   * @return max results value
   */
  public Integer getMaxResults() {
    return maxResults;
  }

  /**
   * Sets the continuation token for pagination.
   *
   * @param continuationToken continuation token returned by the service
   */
  public void setContinuationToken(String continuationToken) {
    this.continuationToken = continuationToken;
  }

  /**
   * Sets the prefix used for filtering container names.
   *
   * @param prefix container name prefix
   */
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  /**
   * Sets the marker indicating the start position of the listing.
   *
   * @param marker listing marker
   */
  public void setMarker(String marker) {
    this.marker = marker;
  }

  /**
   * Sets the maximum number of results requested per page.
   *
   * @param maxResults maximum results value
   */
  public void setMaxResults(Integer maxResults) {
    this.maxResults = maxResults;
  }
}

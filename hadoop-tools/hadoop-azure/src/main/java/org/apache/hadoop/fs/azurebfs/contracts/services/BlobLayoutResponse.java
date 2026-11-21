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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents the response for a Blob layout operation, containing information about
 * the ranges of blobs and their associated endpoints, as well as pagination details.
 */
public class BlobLayoutResponse {

  /**
   * List of ranges representing segments of the blob and their endpoint indices.
   */
  private List<Range> ranges = new ArrayList<>();

  /**
   * Set of endpoints associated with the blob ranges.
   */
  private Set<Endpoint> endpoints = new HashSet<>();

  /**
   * Marker for pagination to retrieve the next set of results.
   */
  private String nextMarker;

  /**
   * Maximum number of results returned in the response.
   */
  private String maxResults;

  /**
   * Represents a range within the blob, defined by start and end positions and the endpoint index.
   * @param start the start position of the range
   * @param end the end position of the range
   * @param endpointIndex the index of the endpoint associated with this range
   */
  public record Range(long start, long end, int endpointIndex) {
    @Override
    public String toString() {
      return "Range{"
          + "Start=" + start
          + ", End=" + end
          + ", EndpointIndex=" + endpointIndex
          + '}';
    }
  }

  /**
   * Represents an endpoint with an index and value.
   * @param index the index of the endpoint
   * @param value the value (e.g., URI) of the endpoint
   */
  public record Endpoint(int index, String value) {
    @Override
    public String toString() {
      return "Endpoint{"
          + "Index=" + index
          + ", Value='" + value + '\''
          + '}';
    }
  }

  /**
   * Gets the set of endpoints associated with this response.
   * @return the set of endpoints
   */
  public Set<Endpoint> getEndpoints() {
    return endpoints;
  }

  /**
   * Sets the endpoints for this response.
   * @param endpoints the set of endpoints to set
   */
  public void setEndpoints(final Set<Endpoint> endpoints) {
    this.endpoints = endpoints;
  }

  /**
   * Adds a single endpoint to the set of endpoints.
   * @param endpoint the endpoint to add
   */
  public void addEndpoint(final Endpoint endpoint) {
    this.endpoints.add(endpoint);
  }

  /**
   * Gets the next marker for pagination.
   * @return the next marker string
   */
  public String getNextMarker() {
    return nextMarker;
  }

  /**
   * Sets the next marker for pagination.
   * @param nextMarker the next marker string to set
   */
  public void setNextMarker(final String nextMarker) {
    this.nextMarker = nextMarker;
  }

  /**
   * Gets the maximum number of results returned in the response.
   * @return the max results string
   */
  public String getMaxResults() {
    return maxResults;
  }

  /**
   * Sets the maximum number of results for the response.
   * @param maxResults the max results string to set
   */
  public void setMaxResults(final String maxResults) {
    this.maxResults = maxResults;
  }

  /**
   * Gets the list of blob ranges in this response.
   * @return the list of ranges
   */
  public List<Range> getRanges() {
    return ranges;
  }

  /**
   * Sets the list of blob ranges for this response.
   * @param ranges the list of ranges to set
   */
  public void setRanges(final List<Range> ranges) {
    this.ranges = ranges;
  }

  /**
   * Adds a single range to the list of blob ranges.
   * @param range the range to add
   */
  public void addRange(final Range range) {
    this.ranges.add(range);
  }

  /**
   * Adds a list of ranges to the existing list.
   * @param newRanges the list of ranges to add
   */
  public void addRanges(final List<Range> newRanges) {
    if (newRanges != null) {
      this.ranges.addAll(newRanges);
    }
  }

  /**
   * Merges another BlobLayoutResponse into this one, replacing ranges, endpoints, nextMarker, and maxResults.
   * @param newResp the new BlobLayoutResponse to merge
   */
  public void addBlobLayoutResponse(BlobLayoutResponse newResp) {
    if (newResp == null) {
      return;
    }

    // Merge ranges by appending all from the new response
    if (newResp.getRanges() != null) {
      this.ranges.addAll(newResp.getRanges());
    }

    // Merge endpoints into the existing set (Set handles duplicates)
    if (newResp.getEndpoints() != null) {
      this.endpoints.addAll(newResp.getEndpoints());
    }

    // Update pagination markers to the latest state
    this.nextMarker = newResp.getNextMarker();
    this.maxResults = newResp.getMaxResults();
  }
}

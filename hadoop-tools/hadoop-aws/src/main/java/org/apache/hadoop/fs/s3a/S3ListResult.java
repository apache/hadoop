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

package org.apache.hadoop.fs.s3a;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;

/**
 * API version-independent container for S3 List responses.
 */
public class S3ListResult {
  private ObjectListing v1Result;
  private ListObjectsV2Result v2Result;

  protected S3ListResult(ObjectListing v1, ListObjectsV2Result v2) {
    v1Result = v1;
    v2Result = v2;
  }

  /**
   * Restricted constructors to ensure v1 or v2, not both.
   * @param result v1 result
   * @return new list result container
   */
  public static S3ListResult v1(ObjectListing result) {
    return new S3ListResult(result, null);
  }

  /**
   * Restricted constructors to ensure v1 or v2, not both.
   * @param result v2 result
   * @return new list result container
   */
  public static S3ListResult v2(ListObjectsV2Result result) {
    return new S3ListResult(null, result);
  }

  /**
   * Is this a v1 API result or v2?
   * @return true if v1, false if v2
   */
  public boolean isV1() {
    return v1Result != null;
  }

  public ObjectListing getV1() {
    return v1Result;
  }

  public ListObjectsV2Result getV2() {
    return v2Result;
  }

  public List<S3ObjectSummary> getObjectSummaries() {
    if (isV1()) {
      return v1Result.getObjectSummaries();
    } else {
      return v2Result.getObjectSummaries();
    }
  }

  public boolean isTruncated() {
    if (isV1()) {
      return v1Result.isTruncated();
    } else {
      return v2Result.isTruncated();
    }
  }

  public List<String> getCommonPrefixes() {
    if (isV1()) {
      return v1Result.getCommonPrefixes();
    } else {
      return v2Result.getCommonPrefixes();
    }
  }

  /**
   * Get the list of keys in the object summary.
   * @return a possibly empty list
   */
  private List<String> objectSummaryKeys() {
    return getObjectSummaries().stream()
        .map(S3ObjectSummary::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Does this listing have prefixes or objects?
   * @return true if the result is non-empty
   */
  public boolean hasPrefixesOrObjects() {

    return !(getCommonPrefixes()).isEmpty()
        || !getObjectSummaries().isEmpty();
  }

  /**
   * Does this listing represent an empty directory?
   * @param dirKey directory key
   * @return true if the list is considered empty.
   */
  public boolean representsEmptyDirectory(
      final String dirKey) {
    // If looking for an empty directory, the marker must exist but
    // no children.
    // So the listing must contain the marker entry only as an object,
    // and prefixes is null
    List<String> keys = objectSummaryKeys();
    return keys.size() == 1 && keys.contains(dirKey)
        && getCommonPrefixes().isEmpty();
  }

  /**
   * Dump the result at debug level.
   * @param log log to use
   */
  public void logAtDebug(Logger log) {
    Collection<String> prefixes = getCommonPrefixes();
    Collection<S3ObjectSummary> summaries = getObjectSummaries();
    log.debug("Prefix count = {}; object count={}",
        prefixes.size(), summaries.size());
    for (S3ObjectSummary summary : summaries) {
      log.debug("Summary: {} {}", summary.getKey(), summary.getSize());
    }
    for (String prefix : prefixes) {
      log.debug("Prefix: {}", prefix);
    }
  }
}

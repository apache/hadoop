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

import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.List;

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
}

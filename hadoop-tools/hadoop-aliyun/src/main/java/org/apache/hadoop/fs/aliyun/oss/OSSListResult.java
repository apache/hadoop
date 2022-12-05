/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.model.ListObjectsV2Result;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;

/**
 * API version-independent container for OSS List responses.
 */
public final class OSSListResult {
  private ObjectListing v1Result;
  private ListObjectsV2Result v2Result;

  protected OSSListResult(ObjectListing v1, ListObjectsV2Result v2) {
    v1Result = v1;
    v2Result = v2;
  }

  /**
   * Restricted constructors to ensure v1 or v2, not both.
   * @param result v1 result
   * @return new list result container
   */
  public static OSSListResult v1(ObjectListing result) {
    return new OSSListResult(result, null);
  }

  /**
   * Restricted constructors to ensure v1 or v2, not both.
   * @param result v2 result
   * @return new list result container
   */
  public static OSSListResult v2(ListObjectsV2Result result) {
    return new OSSListResult(null, result);
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

  public List<OSSObjectSummary> getObjectSummaries() {
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
   * Dump the result at debug level.
   * @param log log to use
   */
  public void logAtDebug(Logger log) {
    Collection<String> prefixes = getCommonPrefixes();
    Collection<OSSObjectSummary> summaries = getObjectSummaries();
    log.debug("Prefix count = {}; object count={}",
        prefixes.size(), summaries.size());
    for (OSSObjectSummary summary : summaries) {
      log.debug("Summary: {} {}", summary.getKey(), summary.getSize());
    }
    for (String prefix : prefixes) {
      log.debug("Prefix: {}", prefix);
    }
  }
}
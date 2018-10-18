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

package org.apache.hadoop.ozone.s3.endpoint;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.s3.commontypes.BucketMetadata;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Response from the ListBucket RPC Call.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListAllMyBucketsResult",
    namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListBucketResponse {
  @XmlElementWrapper(name = "Buckets")
  @XmlElement(name = "Bucket")
  private List<BucketMetadata> buckets = new ArrayList<>();

  public List<BucketMetadata> getBuckets() {
    return buckets;
  }

  @VisibleForTesting
  public int getBucketsNum() {
    return buckets.size();
  }

  public void setBuckets(List<BucketMetadata> buckets) {
    this.buckets = buckets;
  }

  public void addBucket(BucketMetadata bucket) {
    buckets.add(bucket);
  }
}

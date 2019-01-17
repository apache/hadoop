/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;

/**
 * Ozone volume with in-memory state for testing.
 */
public class OzoneVolumeStub extends OzoneVolume {

  private Map<String, OzoneBucketStub> buckets = new HashMap<>();

  public OzoneVolumeStub(String name, String admin, String owner,
      long quotaInBytes,
      long creationTime, List<OzoneAcl> acls) {
    super(name, admin, owner, quotaInBytes, creationTime, acls);
  }

  @Override
  public void createBucket(String bucketName) throws IOException {
    createBucket(bucketName, new BucketArgs.Builder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false)
        .build());
  }

  @Override
  public void createBucket(String bucketName, BucketArgs bucketArgs)
      throws IOException {
    buckets.put(bucketName, new OzoneBucketStub(
        getName(),
        bucketName,
        bucketArgs.getAcls(),
        bucketArgs.getStorageType(),
        bucketArgs.getVersioning(),
        System.currentTimeMillis()));

  }

  @Override
  public OzoneBucket getBucket(String bucketName) throws IOException {
    if (buckets.containsKey(bucketName)) {
      return buckets.get(bucketName);
    } else {
      throw new IOException("BUCKET_NOT_FOUND");
    }

  }

  @Override
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix) {
    return buckets.values()
        .stream()
        .filter(bucket -> {
          if (bucketPrefix != null) {
            return bucket.getName().startsWith(bucketPrefix);
          } else {
            return true;
          }
        })
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneBucket> listBuckets(String bucketPrefix,
      String prevBucket) {
    return buckets.values()
        .stream()
        .filter(bucket -> bucket.getName().compareTo(prevBucket) > 0)
        .filter(bucket -> bucket.getName().startsWith(bucketPrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public void deleteBucket(String bucketName) throws IOException {
    if (buckets.containsKey(bucketName)) {
      buckets.remove(bucketName);
    } else {
      throw new IOException("BUCKET_NOT_FOUND");
    }
  }
}

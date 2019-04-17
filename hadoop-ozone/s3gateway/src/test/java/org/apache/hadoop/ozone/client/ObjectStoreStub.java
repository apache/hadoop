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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.om.exceptions.OMException;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_EMPTY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.S3_BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

/**
 * ObjectStore implementation with in-memory state.
 */
public class ObjectStoreStub extends ObjectStore {

  public ObjectStoreStub() {
    super();
  }

  private Map<String, OzoneVolumeStub> volumes = new HashMap<>();
  private Map<String, String> bucketVolumeMap = new HashMap<>();
  private Map<String, Boolean> bucketEmptyStatus = new HashMap<>();
  private Map<String, List<OzoneBucket>> userBuckets = new HashMap<>();

  @Override
  public void createVolume(String volumeName) throws IOException {
    createVolume(volumeName,
        VolumeArgs.newBuilder()
            .setAdmin("root")
            .setOwner("root")
            .setQuota("" + Integer.MAX_VALUE)
            .setAcls(new ArrayList<>()).build());
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs volumeArgs)
      throws IOException {
    OzoneVolumeStub volume =
        new OzoneVolumeStub(volumeName,
            volumeArgs.getAdmin(),
            volumeArgs.getOwner(),
            Long.parseLong(volumeArgs.getQuota()),
            System.currentTimeMillis(),
            volumeArgs.getAcls());
    volumes.put(volumeName, volume);
  }

  @Override
  public OzoneVolume getVolume(String volumeName) throws IOException {
    if (volumes.containsKey(volumeName)) {
      return volumes.get(volumeName);
    } else {
      throw new OMException("", VOLUME_NOT_FOUND);
    }
  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix)
      throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();

  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix,
      String prevVolume) throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().compareTo(prevVolume) > 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumesByUser(String user,
      String volumePrefix, String prevVolume) throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getOwner().equals(user))
        .filter(volume -> volume.getName().compareTo(prevVolume) < 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public void deleteVolume(String volumeName) throws IOException {
    volumes.remove(volumeName);
  }

  @Override
  public void createS3Bucket(String userName, String s3BucketName) throws
      IOException {
    String volumeName = "s3" + userName;
    if (bucketVolumeMap.get(s3BucketName) == null) {
      bucketVolumeMap.put(s3BucketName, volumeName + "/" + s3BucketName);
      bucketEmptyStatus.put(s3BucketName, true);
      createVolume(volumeName);
      volumes.get(volumeName).createBucket(s3BucketName);
    } else {
      throw new OMException("", BUCKET_ALREADY_EXISTS);
    }

    if (userBuckets.get(userName) == null) {
      List<OzoneBucket> ozoneBuckets = new ArrayList<>();
      ozoneBuckets.add(volumes.get(volumeName).getBucket(s3BucketName));
      userBuckets.put(userName, ozoneBuckets);
    } else {
      userBuckets.get(userName).add(volumes.get(volumeName).getBucket(
          s3BucketName));
    }
  }

  public Iterator<? extends OzoneBucket> listS3Buckets(String userName,
                                                       String bucketPrefix) {
    if (userBuckets.get(userName) == null) {
      return new ArrayList<OzoneBucket>().iterator();
    } else {
      return userBuckets.get(userName).parallelStream()
          .filter(ozoneBucket -> {
            if (bucketPrefix != null) {
              return ozoneBucket.getName().startsWith(bucketPrefix);
            } else {
              return true;
            }
          }).collect(Collectors.toList())
          .iterator();
    }
  }

  public Iterator<? extends OzoneBucket> listS3Buckets(String userName,
                                                       String bucketPrefix,
                                                       String prevBucket) {

    if (userBuckets.get(userName) == null) {
      return new ArrayList<OzoneBucket>().iterator();
    } else {
      //Sort buckets lexicographically
      userBuckets.get(userName).sort(
          (bucket1, bucket2) -> {
            int compare = bucket1.getName().compareTo(bucket2.getName());
            if (compare < 0) {
              return -1;
            } else if (compare == 0) {
              return 0;
            } else {
              return 1;
            }
          });
      return userBuckets.get(userName).stream()
          .filter(ozoneBucket -> {
            if (prevBucket != null) {
              return ozoneBucket.getName().compareTo(prevBucket) > 0;
            } else {
              return true;
            }
          })
          .filter(ozoneBucket -> {
            if (bucketPrefix != null) {
              return ozoneBucket.getName().startsWith(bucketPrefix);
            } else {
              return true;
            }
          }).collect(Collectors.toList())
          .iterator();
    }
  }

  @Override
  public void deleteS3Bucket(String s3BucketName) throws
      IOException {
    if (bucketVolumeMap.containsKey(s3BucketName)) {
      if (bucketEmptyStatus.get(s3BucketName)) {
        bucketVolumeMap.remove(s3BucketName);
      } else {
        throw new OMException("", BUCKET_NOT_EMPTY);
      }
    } else {
      throw new OMException("", BUCKET_NOT_FOUND);
    }
  }

  @Override
  public String getOzoneBucketMapping(String s3BucketName) throws IOException {
    if (bucketVolumeMap.get(s3BucketName) == null) {
      throw new OMException("", S3_BUCKET_NOT_FOUND);
    }
    return bucketVolumeMap.get(s3BucketName);
  }

  @Override
  @SuppressWarnings("StringSplitter")
  public String getOzoneVolumeName(String s3BucketName) throws IOException {
    if (bucketVolumeMap.get(s3BucketName) == null) {
      throw new OMException("", S3_BUCKET_NOT_FOUND);
    }
    return bucketVolumeMap.get(s3BucketName).split("/")[0];
  }

  @Override
  @SuppressWarnings("StringSplitter")
  public String getOzoneBucketName(String s3BucketName) throws IOException {
    if (bucketVolumeMap.get(s3BucketName) == null) {
      throw new OMException("", BUCKET_NOT_FOUND);
    }
    return bucketVolumeMap.get(s3BucketName).split("/")[1];
  }

  public void setBucketEmptyStatus(String bucketName, boolean status) {
    bucketEmptyStatus.put(bucketName, status);
  }
}

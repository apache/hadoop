/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * Utility to help to generate test data.
 */
public final class TestDataUtil {

  private TestDataUtil() {
  }

  public static OzoneBucket createVolumeAndBucket(MiniOzoneCluster cluster,
      String volumeName, String bucketName) throws IOException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);

    OzoneClient client = cluster.getClient();

    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setAdmin(adminName)
        .setOwner(userName)
        .build();

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName, volumeArgs);

    OzoneVolume volume = objectStore.getVolume(volumeName);

    BucketArgs omBucketArgs = BucketArgs.newBuilder()
        .setStorageType(StorageType.DISK)
        .build();

    volume.createBucket(bucketName, omBucketArgs);
    return volume.getBucket(bucketName);

  }

  public static void createKey(OzoneBucket bucket, String keyName,
      String content) throws IOException {
    try (OutputStream stream = bucket
        .createKey(keyName, content.length(), ReplicationType.STAND_ALONE,
            ReplicationFactor.ONE, new HashMap<>())) {
      stream.write(content.getBytes());
    }
  }

  public static String getKey(OzoneBucket bucket, String keyName)
      throws IOException {
    try (InputStream stream = bucket.readKey(keyName)) {
      return new Scanner(stream).useDelimiter("\\A").next();
    }
  }

  public static OzoneBucket createVolumeAndBucket(MiniOzoneCluster cluster)
      throws IOException {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    return createVolumeAndBucket(cluster, volumeName, bucketName);
  }
}

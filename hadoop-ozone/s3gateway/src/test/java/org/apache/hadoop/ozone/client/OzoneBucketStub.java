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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

/**
 * In-memory ozone bucket for testing.
 */
public class OzoneBucketStub extends OzoneBucket {

  private Map<String, OzoneKeyDetails> keyDetails = new HashMap<>();

  private Map<String, byte[]> keyContents = new HashMap<>();

  /**
   * Constructs OzoneBucket instance.
   *
   * @param volumeName   Name of the volume the bucket belongs to.
   * @param bucketName   Name of the bucket.
   * @param acls         ACLs associated with the bucket.
   * @param storageType  StorageType of the bucket.
   * @param versioning   versioning status of the bucket.
   * @param creationTime creation time of the bucket.
   */
  public OzoneBucketStub(
      String volumeName,
      String bucketName,
      List<OzoneAcl> acls,
      StorageType storageType, Boolean versioning,
      long creationTime) {
    super(volumeName,
        bucketName,
        ReplicationFactor.ONE,
        ReplicationType.STAND_ALONE,
        acls,
        storageType,
        versioning,
        creationTime);
  }

  @Override
  public OzoneOutputStream createKey(String key, long size) throws IOException {
    return createKey(key, size, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE);
  }

  @Override
  public OzoneOutputStream createKey(String key, long size,
      ReplicationType type, ReplicationFactor factor) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream =
        new ByteArrayOutputStream((int) size) {
          @Override
          public void close() throws IOException {
            keyContents.put(key, toByteArray());
            keyDetails.put(key, new OzoneKeyDetails(
                getVolumeName(),
                getName(),
                key,
                size,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                new ArrayList<>()
            ));
            super.close();
          }
        };
    return new OzoneOutputStream(byteArrayOutputStream);
  }

  @Override
  public OzoneInputStream readKey(String key) throws IOException {
    return new OzoneInputStream(new ByteArrayInputStream(keyContents.get(key)));
  }

  @Override
  public OzoneKeyDetails getKey(String key) throws IOException {
    return keyDetails.get(key);
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix) {
    return keyDetails.values()
        .stream()
        .filter(key -> key.getName().startsWith(keyPrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneKey> listKeys(String keyPrefix,
      String prevKey) {
    return keyDetails.values()
        .stream()
        .filter(key -> key.getName().compareTo(prevKey) > 0)
        .filter(key -> key.getName().startsWith(keyPrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public void deleteKey(String key) throws IOException {
    keyDetails.remove(key);
  }

  @Override
  public void renameKey(String fromKeyName, String toKeyName)
      throws IOException {
    throw new UnsupportedOperationException();
  }
}

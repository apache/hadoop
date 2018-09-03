/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.BlockNotCommittedException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.rest.response.*;

import java.util.ArrayList;
import java.util.List;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** A utility class for OzoneClient. */
public final class OzoneClientUtils {

  private OzoneClientUtils() {}

  /**
   * Returns a BucketInfo object constructed using fields of the input
   * OzoneBucket object.
   *
   * @param bucket OzoneBucket instance from which BucketInfo object needs to
   *               be created.
   * @return BucketInfo instance
   */
  public static BucketInfo asBucketInfo(OzoneBucket bucket) {
    BucketInfo bucketInfo =
        new BucketInfo(bucket.getVolumeName(), bucket.getName());
    bucketInfo
        .setCreatedOn(HddsClientUtils.formatDateTime(bucket.getCreationTime()));
    bucketInfo.setStorageType(bucket.getStorageType());
    bucketInfo.setVersioning(
        OzoneConsts.Versioning.getVersioning(bucket.getVersioning()));
    bucketInfo.setAcls(bucket.getAcls());
    return bucketInfo;
  }

  /**
   * Returns a VolumeInfo object constructed using fields of the input
   * OzoneVolume object.
   *
   * @param volume OzoneVolume instance from which VolumeInfo object needs to
   *               be created.
   * @return VolumeInfo instance
   */
  public static VolumeInfo asVolumeInfo(OzoneVolume volume) {
    VolumeInfo volumeInfo = new VolumeInfo(volume.getName(),
        HddsClientUtils.formatDateTime(volume.getCreationTime()),
        volume.getOwner());
    volumeInfo.setQuota(OzoneQuota.getOzoneQuota(volume.getQuota()));
    volumeInfo.setOwner(new VolumeOwner(volume.getOwner()));
    return volumeInfo;
  }

  /**
   * Returns a KeyInfo object constructed using fields of the input
   * OzoneKey object.
   *
   * @param key OzoneKey instance from which KeyInfo object needs to
   *            be created.
   * @return KeyInfo instance
   */
  public static KeyInfo asKeyInfo(OzoneKey key) {
    KeyInfo keyInfo = new KeyInfo();
    keyInfo.setKeyName(key.getName());
    keyInfo.setCreatedOn(HddsClientUtils.formatDateTime(key.getCreationTime()));
    keyInfo.setModifiedOn(
        HddsClientUtils.formatDateTime(key.getModificationTime()));
    keyInfo.setSize(key.getDataSize());
    return keyInfo;
  }

  public static RetryPolicy createRetryPolicy(Configuration conf) {
    int maxRetryCount =
        conf.getInt(OzoneConfigKeys.OZONE_CLIENT_MAX_RETRIES, OzoneConfigKeys.
            OZONE_CLIENT_MAX_RETRIES_DEFAULT);
    long retryInterval = conf.getTimeDuration(OzoneConfigKeys.
        OZONE_CLIENT_RETRY_INTERVAL, OzoneConfigKeys.
        OZONE_CLIENT_RETRY_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    RetryPolicy basePolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(maxRetryCount, retryInterval,
            TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(BlockNotCommittedException.class, basePolicy);
    RetryPolicy retryPolicy = RetryPolicies
        .retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            exceptionToPolicyMap);
    return retryPolicy;
  }
  /**
   * Returns a KeyInfoDetails object constructed using fields of the input
   * OzoneKeyDetails object.
   *
   * @param key OzoneKeyDetails instance from which KeyInfo object needs to
   *            be created.
   * @return KeyInfoDetails instance
   */
  public static KeyInfoDetails asKeyInfoDetails(OzoneKeyDetails key) {
    KeyInfoDetails keyInfo = new KeyInfoDetails();
    keyInfo.setKeyName(key.getName());
    keyInfo.setCreatedOn(HddsClientUtils.formatDateTime(key.getCreationTime()));
    keyInfo.setModifiedOn(
        HddsClientUtils.formatDateTime(key.getModificationTime()));
    keyInfo.setSize(key.getDataSize());
    List<KeyLocation> keyLocations = new ArrayList<>();
    key.getOzoneKeyLocations().forEach((a) -> keyLocations.add(new KeyLocation(
        a.getContainerID(), a.getLocalID(), a.getLength(), a.getOffset())));
    keyInfo.setKeyLocation(keyLocations);
    return keyInfo;
  }
}

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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper around {@link com.amazonaws.services.s3.AmazonS3} that injects
 * inconsistency and/or errors.  Used for testing S3Guard.
 * Currently only delays listing visibility, not affecting GET.
 */
public class InconsistentAmazonS3Client extends AmazonS3Client {

  /**
   * Keys containing this substring will be subject to delayed visibility.
   */
  public static final String DELAY_KEY_SUBSTRING = "DELAY_LISTING_ME";

  /**
   * How many seconds affected keys will be delayed from appearing in listing.
   * This should probably be a config value.
   */
  public static final long DELAY_KEY_MILLIS = 5 * 1000;

  private static final Logger LOG =
      LoggerFactory.getLogger(InconsistentAmazonS3Client.class);

  /** Map of key to delay -> time it was created. */
  private Map<String, Long> delayedKeys = new HashMap<>();

  public InconsistentAmazonS3Client(AWSCredentialsProvider credentials,
      ClientConfiguration clientConfiguration) {
    super(credentials, clientConfiguration);
  }

  /* We should only need to override this version of putObject() */
  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest)
      throws AmazonClientException, AmazonServiceException {
    LOG.debug("key {}", putObjectRequest.getKey());
    registerPutObject(putObjectRequest);
    return super.putObject(putObjectRequest);
  }

  /* We should only need to override this version of listObjects() */
  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
      throws AmazonClientException, AmazonServiceException {
    LOG.debug("prefix {}", listObjectsRequest.getPrefix());
    ObjectListing listing = super.listObjects(listObjectsRequest);
    return filterListObjects(listObjectsRequest,
        listing);
  }


  private ObjectListing filterListObjects(ListObjectsRequest request,
      ObjectListing rawListing) {

    // Filter object listing
    List<S3ObjectSummary> outputList = new ArrayList<>();
    for (S3ObjectSummary s : rawListing.getObjectSummaries()) {
      if (!isVisibilityDelayed(s.getKey())) {
        outputList.add(s);
      }
    }

    // Filter prefixes (directories)
    List<String> outputPrefixes = new ArrayList<>();
    for (String key : rawListing.getCommonPrefixes()) {
      if (!isVisibilityDelayed(key)) {
        outputPrefixes.add(key);
      }
    }

    return new CustomObjectListing(rawListing, outputList, outputPrefixes);
  }

  private boolean isVisibilityDelayed(String key) {
    Long createTime = delayedKeys.get(key);
    if (createTime == null) {
      LOG.debug("no delay for key {}", key);
      return false;
    }
    long currentTime = System.currentTimeMillis();
    long deadline = createTime + DELAY_KEY_MILLIS;
    if (currentTime >= deadline) {
      delayedKeys.remove(key);
      LOG.debug("{} no longer delayed", key);
      return false;
    } else  {
      LOG.info("{} delaying visibility", key);
      return true;
    }
  }

  private void registerPutObject(PutObjectRequest req) {
    String key = req.getKey();
    if (shouldDelay(key)) {
      enqueueDelayKey(key);
    }
  }

  /**
   * Should we delay listing visibility for this key?
   * @param key key which is being put
   * @return true if we should delay
   */
  private boolean shouldDelay(String key) {
    boolean delay = key.contains(DELAY_KEY_SUBSTRING);
    LOG.debug("{} -> {}", key, delay);
    return delay;
  }

  /**
   * Record this key as something that should not become visible in
   * listObject replies for a while, to simulate eventual list consistency.
   * @param key key to delay visibility of
   */
  private void enqueueDelayKey(String key) {
    LOG.debug("key {}", key);
    delayedKeys.put(key, System.currentTimeMillis());
  }

  /** Since ObjectListing is immutable, we just override it with wrapper. */
  private static class CustomObjectListing extends ObjectListing {

    private final List<S3ObjectSummary> customListing;
    private final List<String> customPrefixes;

    public CustomObjectListing(ObjectListing rawListing,
        List<S3ObjectSummary> customListing, List<String> customPrefixes) {
      super();
      this.customListing = customListing;
      this.customPrefixes = customPrefixes;

      this.setBucketName(rawListing.getBucketName());
      this.setCommonPrefixes(rawListing.getCommonPrefixes());
      this.setDelimiter(rawListing.getDelimiter());
      this.setEncodingType(rawListing.getEncodingType());
      this.setMarker(rawListing.getMarker());
      this.setMaxKeys(rawListing.getMaxKeys());
      this.setNextMarker(rawListing.getNextMarker());
      this.setPrefix(rawListing.getPrefix());
      this.setTruncated(rawListing.isTruncated());
    }

    @Override
    public List<S3ObjectSummary> getObjectSummaries() {
      return customListing;
    }

    @Override
    public List<String> getCommonPrefixes() {
      return customPrefixes;
    }
  }
}

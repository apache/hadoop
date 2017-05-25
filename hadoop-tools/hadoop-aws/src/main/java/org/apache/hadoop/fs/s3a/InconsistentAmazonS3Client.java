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
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

  /**
   * Composite of data we need to track about recently deleted objects:
   * when it was deleted (same was with recently put objects) and the object
   * summary (since we should keep returning it for sometime after its
   * deletion).
   */
  private static class Delete {
    private Long time;
    private S3ObjectSummary summary;

    Delete(Long time, S3ObjectSummary summary) {
      this.time = time;
      this.summary = summary;
    }

    public Long time() {
      return time;
    }

    public S3ObjectSummary summary() {
      return summary;
    }
  }

  /** Map of key to delay -> time it was deleted + object summary (object
   * summary is null for prefixes. */
  private Map<String, Delete> delayedDeletes = new HashMap<>();

  /** Map of key to delay -> time it was created. */
  private Map<String, Long> delayedPutKeys = new HashMap<>();

  public InconsistentAmazonS3Client(AWSCredentialsProvider credentials,
      ClientConfiguration clientConfiguration) {
    super(credentials, clientConfiguration);
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest)
      throws AmazonClientException, AmazonServiceException {
    LOG.debug("key {}", deleteObjectRequest.getKey());
    registerDeleteObject(deleteObjectRequest);
    super.deleteObject(deleteObjectRequest);
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
    listing = filterListObjects(listObjectsRequest, listing);
    listing = restoreListObjects(listObjectsRequest, listing);
    return listing;
  }

  private boolean addIfNotPresent(List<S3ObjectSummary> list,
      S3ObjectSummary item) {
    // Behavior of S3ObjectSummary
    String key = item.getKey();
    for (S3ObjectSummary member : list) {
      if (member.getKey().equals(key)) {
        return false;
      }
    }
    return list.add(item);
  }

  private ObjectListing restoreListObjects(ListObjectsRequest request,
                                           ObjectListing rawListing) {
    List<S3ObjectSummary> outputList = rawListing.getObjectSummaries();
    List<String> outputPrefixes = rawListing.getCommonPrefixes();
    for (String key : new HashSet<>(delayedDeletes.keySet())) {
      Delete delete = delayedDeletes.get(key);
      if (isKeyDelayed(delete.time(), key)) {
        // TODO this works fine for flat directories but:
        // if you have a delayed key /a/b/c/d and you are listing /a/b,
        // this incorrectly will add /a/b/c/d to the listing for b
        if (key.startsWith(request.getPrefix())) {
          if (delete.summary == null) {
            if (!outputPrefixes.contains(key)) {
              outputPrefixes.add(key);
            }
          } else {
            addIfNotPresent(outputList, delete.summary());
          }
        }
      } else {
        delayedDeletes.remove(key);
      }
    }

    return new CustomObjectListing(rawListing, outputList, outputPrefixes);
  }

  private ObjectListing filterListObjects(ListObjectsRequest request,
      ObjectListing rawListing) {

    // Filter object listing
    List<S3ObjectSummary> outputList = new ArrayList<>();
    for (S3ObjectSummary s : rawListing.getObjectSummaries()) {
      String key = s.getKey();
      if (!isKeyDelayed(delayedPutKeys.get(key), key)) {
        outputList.add(s);
      }
    }

    // Filter prefixes (directories)
    List<String> outputPrefixes = new ArrayList<>();
    for (String key : rawListing.getCommonPrefixes()) {
      if (!isKeyDelayed(delayedPutKeys.get(key), key)) {
        outputPrefixes.add(key);
      }
    }

    return new CustomObjectListing(rawListing, outputList, outputPrefixes);
  }

  private boolean isKeyDelayed(Long enqueueTime, String key) {
    if (enqueueTime == null) {
      LOG.debug("no delay for key {}", key);
      return false;
    }
    long currentTime = System.currentTimeMillis();
    long deadline = enqueueTime + DELAY_KEY_MILLIS;
    if (currentTime >= deadline) {
      delayedDeletes.remove(key);
      LOG.debug("no longer delaying {}", key);
      return false;
    } else  {
      LOG.info("delaying {}", key);
      return true;
    }
  }

  private void registerDeleteObject(DeleteObjectRequest req) {
    String key = req.getKey();
    if (shouldDelay(key)) {
      // Record summary so we can add it back for some time post-deletion
      S3ObjectSummary summary = null;
      ObjectListing list = listObjects(req.getBucketName(), key);
      for (S3ObjectSummary result : list.getObjectSummaries()) {
        if (result.getKey().equals(key)) {
          summary = result;
          break;
        }
      }
      delayedDeletes.put(key, new Delete(System.currentTimeMillis(), summary));
    }
  }

  private void registerPutObject(PutObjectRequest req) {
    String key = req.getKey();
    if (shouldDelay(key)) {
      enqueueDelayedPut(key);
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
  private void enqueueDelayedPut(String key) {
    LOG.debug("delaying put of {}", key);
    delayedPutKeys.put(key, System.currentTimeMillis());
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

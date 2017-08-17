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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.s3a.Constants.*;

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
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InconsistentAmazonS3Client extends AmazonS3Client {

  /**
   * Keys containing this substring will be subject to delayed visibility.
   */
  public static final String DEFAULT_DELAY_KEY_SUBSTRING = "DELAY_LISTING_ME";

  /**
   * How many seconds affected keys will be delayed from appearing in listing.
   * This should probably be a config value.
   */
  public static final long DEFAULT_DELAY_KEY_MSEC = 5 * 1000;

  public static final float DEFAULT_DELAY_KEY_PROBABILITY = 1.0f;

  /** Special config value since we can't store empty strings in XML. */
  public static final String MATCH_ALL_KEYS = "*";

  private static final Logger LOG =
      LoggerFactory.getLogger(InconsistentAmazonS3Client.class);

  /** Empty string matches all keys. */
  private String delayKeySubstring;

  /** Probability to delay visibility of a matching key. */
  private float delayKeyProbability;

  /** Time in milliseconds to delay visibility of newly modified object. */
  private long delayKeyMsec;

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
      ClientConfiguration clientConfiguration, Configuration conf) {
    super(credentials, clientConfiguration);
    setupConfig(conf);
  }

  protected void setupConfig(Configuration conf) {

    delayKeySubstring = conf.get(FAIL_INJECT_INCONSISTENCY_KEY,
        DEFAULT_DELAY_KEY_SUBSTRING);
    // "" is a substring of all strings, use it to match all keys.
    if (delayKeySubstring.equals(MATCH_ALL_KEYS)) {
      delayKeySubstring = "";
    }
    delayKeyProbability = conf.getFloat(FAIL_INJECT_INCONSISTENCY_PROBABILITY,
        DEFAULT_DELAY_KEY_PROBABILITY);
    delayKeyMsec = conf.getLong(FAIL_INJECT_INCONSISTENCY_MSEC,
        DEFAULT_DELAY_KEY_MSEC);
    LOG.info("Enabled with {} msec delay, substring {}, probability {}",
        delayKeyMsec, delayKeySubstring, delayKeyProbability);
  }

  /**
   * Clear all oustanding inconsistent keys.  After calling this function,
   * listings should behave normally (no failure injection), until additional
   * keys are matched for delay, e.g. via putObject(), deleteObject().
   */
  public void clearInconsistency() {
    LOG.info("clearing all delayed puts / deletes");
    delayedDeletes.clear();
    delayedPutKeys.clear();
  }

  /**
   * Convenience function for test code to cast from supertype.
   * @param c supertype to cast from
   * @return subtype, not null
   * @throws Exception on error
   */
  public static InconsistentAmazonS3Client castFrom(AmazonS3 c) throws
      Exception {
    InconsistentAmazonS3Client ic = null;
    if (c instanceof InconsistentAmazonS3Client) {
      ic = (InconsistentAmazonS3Client) c;
    }
    Preconditions.checkNotNull(ic, "Not an instance of " +
        "InconsistentAmazonS3Client");
    return ic;
  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest
      deleteObjectsRequest)
      throws AmazonClientException, AmazonServiceException {
    for (DeleteObjectsRequest.KeyVersion keyVersion :
        deleteObjectsRequest.getKeys()) {
      registerDeleteObject(keyVersion.getKey(), deleteObjectsRequest
          .getBucketName());
    }
    return super.deleteObjects(deleteObjectsRequest);
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest)
      throws AmazonClientException, AmazonServiceException {
    String key = deleteObjectRequest.getKey();
    LOG.debug("key {}", key);
    registerDeleteObject(key, deleteObjectRequest.getBucketName());
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

  private void addSummaryIfNotPresent(List<S3ObjectSummary> list,
      S3ObjectSummary item) {
    // Behavior of S3ObjectSummary
    String key = item.getKey();
    for (S3ObjectSummary member : list) {
      if (member.getKey().equals(key)) {
        return;
      }
    }
    list.add(item);
  }

  /**
   * Add prefix of child to given list.  The added prefix will be equal to
   * ancestor plus one directory past ancestor.  e.g.:
   * if ancestor is "/a/b/c" and child is "/a/b/c/d/e/file" then "a/b/c/d" is
   * added to list.
   * @param prefixes list to add to
   * @param ancestor path we are listing in
   * @param child full path to get prefix from
   */
  private void addPrefixIfNotPresent(List<String> prefixes, String ancestor,
      String child) {
    Path prefixCandidate = new Path(child).getParent();
    Path ancestorPath = new Path(ancestor);
    Preconditions.checkArgument(child.startsWith(ancestor), "%s does not " +
        "start with %s", child, ancestor);
    while (!prefixCandidate.isRoot()) {
      Path nextParent = prefixCandidate.getParent();
      if (nextParent.equals(ancestorPath)) {
        String prefix = prefixCandidate.toString();
        if (!prefixes.contains(prefix)) {
          prefixes.add(prefix);
        }
        return;
      }
      prefixCandidate = nextParent;
    }
  }

  /**
   * Checks that the parent key is an ancestor of the child key.
   * @param parent key that may be the parent.
   * @param child key that may be the child.
   * @param recursive if false, only return true for direct children.  If
   *                  true, any descendant will count.
   * @return true if parent is an ancestor of child
   */
  private boolean isDescendant(String parent, String child, boolean recursive) {
    if (recursive) {
      if (!parent.endsWith("/")) {
        parent = parent + "/";
      }
      return child.startsWith(parent);
    } else {
      Path actualParentPath = new Path(child).getParent();
      Path expectedParentPath = new Path(parent);
      return actualParentPath.equals(expectedParentPath);
    }
  }

  /**
   * Simulate eventual consistency of delete for this list operation:  Any
   * recently-deleted keys will be added.
   * @param request List request
   * @param rawListing listing returned from underlying S3
   * @return listing with recently-deleted items restored
   */
  private ObjectListing restoreListObjects(ListObjectsRequest request,
      ObjectListing rawListing) {
    List<S3ObjectSummary> outputList = rawListing.getObjectSummaries();
    List<String> outputPrefixes = rawListing.getCommonPrefixes();
    // recursive list has no delimiter, returns everything that matches a
    // prefix.
    boolean recursiveObjectList = !("/".equals(request.getDelimiter()));

    // Go through all deleted keys
    for (String key : new HashSet<>(delayedDeletes.keySet())) {
      Delete delete = delayedDeletes.get(key);
      if (isKeyDelayed(delete.time(), key)) {
        if (isDescendant(request.getPrefix(), key, recursiveObjectList)) {
          if (delete.summary() != null) {
            addSummaryIfNotPresent(outputList, delete.summary());
          }
        }
        // Non-recursive list has delimiter: will return rolled-up prefixes for
        // all keys that are not direct children
        if (!recursiveObjectList) {
          if (isDescendant(request.getPrefix(), key, true)) {
            addPrefixIfNotPresent(outputPrefixes, request.getPrefix(), key);
          }
        }
      } else {
        // Clean up any expired entries
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
    long deadline = enqueueTime + delayKeyMsec;
    if (currentTime >= deadline) {
      delayedDeletes.remove(key);
      LOG.debug("no longer delaying {}", key);
      return false;
    } else  {
      LOG.info("delaying {}", key);
      return true;
    }
  }

  private void registerDeleteObject(String key, String bucket) {
    if (shouldDelay(key)) {
      // Record summary so we can add it back for some time post-deletion
      S3ObjectSummary summary = null;
      ObjectListing list = listObjects(bucket, key);
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
    boolean delay = key.contains(delayKeySubstring);
    delay = delay && trueWithProbability(delayKeyProbability);
    LOG.debug("{} -> {}", key, delay);
    return delay;
  }


  private boolean trueWithProbability(float p) {
    return Math.random() < p;
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

    CustomObjectListing(ObjectListing rawListing,
        List<S3ObjectSummary> customListing,
        List<String> customPrefixes) {
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

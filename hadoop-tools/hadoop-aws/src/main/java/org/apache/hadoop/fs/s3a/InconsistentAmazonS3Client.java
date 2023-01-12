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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * A wrapper around {@link com.amazonaws.services.s3.AmazonS3} that injects
 * failures.
 * It used to also inject inconsistency, but this was removed with S3Guard;
 * what is retained is the ability to throttle AWS operations and for the
 * input stream to be inconsistent.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InconsistentAmazonS3Client extends AmazonS3Client {

  private static final Logger LOG =
      LoggerFactory.getLogger(InconsistentAmazonS3Client.class);

  private FailureInjectionPolicy policy;

  /**
   * Counter of failures since last reset.
   */
  private final AtomicLong failureCounter = new AtomicLong(0);


  /**
   * Instantiate.
   * This subclasses a deprecated constructor of the parent
   * {@code AmazonS3Client} class; we can't use the builder API because,
   * that only creates the consistent client.
   * @param credentials credentials to auth.
   * @param clientConfiguration connection settings
   * @param conf hadoop configuration.
   */
  @SuppressWarnings("deprecation")
  public InconsistentAmazonS3Client(AWSCredentialsProvider credentials,
      ClientConfiguration clientConfiguration, Configuration conf) {
    super(credentials, clientConfiguration);
    policy = new FailureInjectionPolicy(conf);
  }

  /**
   * A way for tests to patch in a different fault injection policy at runtime.
   * @param fs filesystem under test
   * @param policy failure injection settings to set
   * @throws Exception on failure
   */
  public static void setFailureInjectionPolicy(S3AFileSystem fs,
      FailureInjectionPolicy policy) throws Exception {
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("s3guard");
    InconsistentAmazonS3Client ic = InconsistentAmazonS3Client.castFrom(s3);
    ic.replacePolicy(policy);
  }

  private void replacePolicy(FailureInjectionPolicy pol) {
    this.policy = pol;
  }

  @Override
  public String toString() {
    return String.format("Inconsistent S3 Client: %s; failure count %d",
        policy, failureCounter.get());
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
    maybeFail();
    return super.deleteObjects(deleteObjectsRequest);
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest)
      throws AmazonClientException, AmazonServiceException {
    String key = deleteObjectRequest.getKey();
    LOG.debug("key {}", key);
    maybeFail();
    super.deleteObject(deleteObjectRequest);
  }

  /* We should only need to override this version of putObject() */
  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest)
      throws AmazonClientException, AmazonServiceException {
    LOG.debug("key {}", putObjectRequest.getKey());
    maybeFail();
    return super.putObject(putObjectRequest);
  }

  /* We should only need to override these versions of listObjects() */
  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
      throws AmazonClientException, AmazonServiceException {
    maybeFail();
    return super.listObjects(listObjectsRequest);
  }

  /* consistent listing with possibility of failing. */
  @Override
  public ListObjectsV2Result listObjectsV2(ListObjectsV2Request request)
      throws AmazonClientException, AmazonServiceException {
    maybeFail();
    return super.listObjectsV2(request);
  }


  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(
      CompleteMultipartUploadRequest completeMultipartUploadRequest)
      throws SdkClientException, AmazonServiceException {
    maybeFail();
    return super.completeMultipartUpload(completeMultipartUploadRequest);
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest uploadPartRequest)
      throws SdkClientException, AmazonServiceException {
    maybeFail();
    return super.uploadPart(uploadPartRequest);
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(
      InitiateMultipartUploadRequest initiateMultipartUploadRequest)
      throws SdkClientException, AmazonServiceException {
    maybeFail();
    return super.initiateMultipartUpload(initiateMultipartUploadRequest);
  }

  @Override
  public MultipartUploadListing listMultipartUploads(
      ListMultipartUploadsRequest listMultipartUploadsRequest)
      throws SdkClientException, AmazonServiceException {
    maybeFail();
    return super.listMultipartUploads(listMultipartUploadsRequest);
  }

  /**
   * Set the probability of throttling a request.
   * @param throttleProbability the probability of a request being throttled.
   */
  public void setThrottleProbability(float throttleProbability) {
    policy.setThrottleProbability(throttleProbability);
  }

  /**
   * Conditionally fail the operation.
   * @param errorMsg description of failure
   * @param statusCode http status code for error
   * @throws AmazonClientException if the client chooses to fail
   * the request.
   */
  private void maybeFail(String errorMsg, int statusCode)
      throws AmazonClientException {
    // code structure here is to line up for more failures later
    AmazonServiceException ex = null;
    if (FailureInjectionPolicy.trueWithProbability(policy.getThrottleProbability())) {
      // throttle the request
      ex = new AmazonServiceException(errorMsg
          + " count = " + (failureCounter.get() + 1), null);
      ex.setStatusCode(statusCode);
    }

    int failureLimit = policy.getFailureLimit();
    if (ex != null) {
      long count = failureCounter.incrementAndGet();
      if (failureLimit == 0
          || (failureLimit > 0 && count < failureLimit)) {
        throw ex;
      }
    }
  }

  private void maybeFail() {
    maybeFail("throttled", 503);
  }

  /**
   * Set the limit on failures before all operations pass through.
   * This resets the failure count.
   * @param limit limit; "0" means "no limit"
   */
  public void setFailureLimit(int limit) {
    policy.setFailureLimit(limit);
    failureCounter.set(0);
  }

  @Override
  public S3Object getObject(GetObjectRequest var1) throws SdkClientException,
      AmazonServiceException {
    maybeFail();
    return super.getObject(var1);
  }

  @Override
  public S3Object getObject(String bucketName, String key)
      throws SdkClientException, AmazonServiceException {
    maybeFail();
    return super.getObject(bucketName, key);

  }

  /** Since ObjectListing is immutable, we just override it with wrapper. */
  @SuppressWarnings("serial")
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

  @SuppressWarnings("serial")
  private static class CustomListObjectsV2Result extends ListObjectsV2Result {

    private final List<S3ObjectSummary> customListing;
    private final List<String> customPrefixes;

    CustomListObjectsV2Result(ListObjectsV2Result raw,
        List<S3ObjectSummary> customListing, List<String> customPrefixes) {
      super();
      this.customListing = customListing;
      this.customPrefixes = customPrefixes;

      this.setBucketName(raw.getBucketName());
      this.setCommonPrefixes(raw.getCommonPrefixes());
      this.setDelimiter(raw.getDelimiter());
      this.setEncodingType(raw.getEncodingType());
      this.setStartAfter(raw.getStartAfter());
      this.setMaxKeys(raw.getMaxKeys());
      this.setContinuationToken(raw.getContinuationToken());
      this.setPrefix(raw.getPrefix());
      this.setTruncated(raw.isTruncated());
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

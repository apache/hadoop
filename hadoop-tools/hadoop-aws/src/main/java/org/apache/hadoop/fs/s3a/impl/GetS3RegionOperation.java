/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.UnknownStoreException;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.Constants.BUCKET_REGION_HEADER;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_BUCKET_PREFIX;
import static org.apache.hadoop.fs.s3a.Statistic.STORE_REGION_PROBE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_301_MOVED_PERMANENTLY;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404_NOT_FOUND;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;

/**
 * Operation to get the S3 region for a bucket.
 * If a region is not specified in {@link org.apache.hadoop.fs.s3a.Constants#AWS_REGION}, this
 * operation will warn and then probe S3 to get the region.
 */
public class GetS3RegionOperation extends ExecutingStoreOperation<Region> {

  private final AWSCredentialProviderList credentials;

  private final String region;

  private final String bucket;

  private final StoreContext context;

  private final static Map<String, Region> BUCKET_REGIONS = new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(
      GetS3RegionOperation.class);

  /** Exactly once log to warn about setting the region in config to avoid probe. */
  private static final LogExactlyOnce SET_REGION_WARNING = new LogExactlyOnce(LOG);

  public GetS3RegionOperation(
      final StoreContext context,
      String region,
      String bucket,
      AWSCredentialProviderList credentials) {
      super(context);
      this.context = context;
      this.credentials = credentials;
      this.region = region;
      this.bucket = bucket;
  }


  @Override
  public Region execute() throws IOException {
    
    if (!StringUtils.isBlank(region)) {
      return Region.of(region);
    }

    Region cachedRegion = BUCKET_REGIONS.get(bucket);

    if (cachedRegion != null) {
      LOG.debug("Got region {} for bucket {} from cache", cachedRegion, bucket);
      return cachedRegion;
    }

    try (DurationInfo ignore =
        new DurationInfo(LOG, false, "Get S3 region")) {

      SET_REGION_WARNING.warn(
          "Getting region for bucket {} from S3, this will slow down FS initialisation. "
              + "To avoid this, set the region using property {}", bucket,
          FS_S3A_BUCKET_PREFIX + bucket + ".endpoint.region");

      // build a s3 client with region eu-west-1 that can be used to get the region of the
      // bucket. Using eu-west-1, as headBucket() doesn't work with us-east-1. This is because
      // us-east-1 uses the endpoint s3.amazonaws.com, which resolves bucket.s3.amazonaws.com
      // to the actual region the bucket is in. As the request is signed with us-east-1 and
      // not the bucket's region, it fails.
      S3Client getRegionS3Client =
          S3Client.builder().region(Region.EU_WEST_1).credentialsProvider(credentials)
              .build();

      HeadBucketResponse headBucketResponse = trackDuration(context.getInstrumentation(),
          STORE_REGION_PROBE.getSymbol(),
          () -> getRegionS3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build()));

      Region bucketRegion = Region.of(
          headBucketResponse.sdkHttpResponse().headers().get(BUCKET_REGION_HEADER).get(0));
      BUCKET_REGIONS.put(bucket, bucketRegion);

      return bucketRegion;
    } catch (S3Exception exception) {
      if (exception.statusCode() == SC_301_MOVED_PERMANENTLY) {
        Region bucketRegion = Region.of(
            exception.awsErrorDetails().sdkHttpResponse().headers().get(BUCKET_REGION_HEADER)
                .get(0));
        BUCKET_REGIONS.put(bucket, bucketRegion);

        return bucketRegion;
      }

      if (exception.statusCode() == SC_404_NOT_FOUND) {
        throw new UnknownStoreException("s3a://" + bucket + "/",
            " Bucket does " + "not exist");
      }

      throw exception;
    }
  }
}

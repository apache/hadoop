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

import java.io.IOException;
import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.slf4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;

/**
 * The default {@link S3ClientFactory} implementation.
 * This which calls the AWS SDK to configure and create an
 * {@link AmazonS3Client} that communicates with the S3 service.
 */
public class DefaultS3ClientFactory extends Configured
    implements S3ClientFactory {

  protected static final Logger LOG = S3AFileSystem.LOG;

  @Override
  public AmazonS3 createS3Client(URI name,
      final String bucket,
      final AWSCredentialsProvider credentials) throws IOException {
    Configuration conf = getConf();
    final ClientConfiguration awsConf = S3AUtils.createAwsConf(getConf(), bucket);
    return configureAmazonS3Client(
        newAmazonS3Client(credentials, awsConf), conf);
  }

  /**
   * Wrapper around constructor for {@link AmazonS3} client.
   * Override this to provide an extended version of the client
   * @param credentials credentials to use
   * @param awsConf  AWS configuration
   * @return  new AmazonS3 client
   */
  protected AmazonS3 newAmazonS3Client(
      AWSCredentialsProvider credentials, ClientConfiguration awsConf) {
    return new AmazonS3Client(credentials, awsConf);
  }

  /**
   * Configure S3 client from the Hadoop configuration.
   *
   * This includes: endpoint, Path Access and possibly other
   * options.
   *
   * @param conf Hadoop configuration
   * @return S3 client
   * @throws IllegalArgumentException if misconfigured
   */
  private static AmazonS3 configureAmazonS3Client(AmazonS3 s3,
      Configuration conf)
      throws IllegalArgumentException {
    String endPoint = conf.getTrimmed(ENDPOINT, "");
    if (!endPoint.isEmpty()) {
      try {
        s3.setEndpoint(endPoint);
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: "  + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }
    }
    return applyS3ClientOptions(s3, conf);
  }

  /**
   * Perform any tuning of the {@code S3ClientOptions} settings based on
   * the Hadoop configuration.
   * This is different from the general AWS configuration creation as
   * it is unique to S3 connections.
   *
   * The {@link Constants#PATH_STYLE_ACCESS} option enables path-style access
   * to S3 buckets if configured.  By default, the
   * behavior is to use virtual hosted-style access with URIs of the form
   * {@code http://bucketname.s3.amazonaws.com}
   * Enabling path-style access and a
   * region-specific endpoint switches the behavior to use URIs of the form
   * {@code http://s3-eu-west-1.amazonaws.com/bucketname}.
   * It is common to use this when connecting to private S3 servers, as it
   * avoids the need to play with DNS entries.
   * @param s3 S3 client
   * @param conf Hadoop configuration
   * @return the S3 client
   */
  private static AmazonS3 applyS3ClientOptions(AmazonS3 s3,
      Configuration conf) {
    final boolean pathStyleAccess = conf.getBoolean(PATH_STYLE_ACCESS, false);
    if (pathStyleAccess) {
      LOG.debug("Enabling path style access!");
      s3.setS3ClientOptions(S3ClientOptions.builder()
          .setPathStyleAccess(true)
          .build());
    }
    return s3;
  }
}

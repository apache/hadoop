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

package org.apache.hadoop.fs.s3a.auth;

import java.io.IOException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;

/**
 * Factory for creating STS Clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class STSClientFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(STSClientFactory.class);

  /**
   * Create the builder ready for any final configuration options.
   * Picks up connection settings from the Hadoop configuration, including
   * proxy secrets.
   * @param conf Configuration to act as source of options.
   * @param bucket Optional bucket to use to look up per-bucket proxy secrets
   * @param credentials AWS credential chain to use
   * @param stsEndpoint optional endpoint "https://sns.us-west-1.amazonaws.com"
   * @param stsRegion the region, e.g "us-west-1"
   * @return the builder to call {@code build()}
   * @throws IOException problem reading proxy secrets
   */
  public static AWSSecurityTokenServiceClientBuilder builder(
      final Configuration conf,
      final String bucket,
      final AWSCredentialsProvider credentials, final String stsEndpoint,
      final String stsRegion) throws IOException {
    Preconditions.checkArgument(credentials != null, "No credentials");
    final AWSSecurityTokenServiceClientBuilder builder
        = AWSSecurityTokenServiceClientBuilder.standard();
    final ClientConfiguration awsConf = S3AUtils.createAwsConf(conf, bucket);
    builder.withClientConfiguration(awsConf);
    builder.withCredentials(credentials);
    if (StringUtils.isNotEmpty(stsEndpoint)) {
      LOG.debug("STS Endpoint ={}", stsEndpoint);
      builder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(stsEndpoint, stsRegion));
    }
    return builder;
  }

}

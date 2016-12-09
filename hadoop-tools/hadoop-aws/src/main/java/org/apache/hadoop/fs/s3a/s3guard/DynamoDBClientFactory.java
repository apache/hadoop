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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.s3.model.Region;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;

import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet;

/**
 * Interface to create a DynamoDB client.
 *
 * Implementation should be configured for setting and getting configuration.
 */
interface DynamoDBClientFactory extends Configurable {
  Logger LOG = LoggerFactory.getLogger(DynamoDBClientFactory.class);

  /**
   * To create a DynamoDB client with the same region as the s3 bucket.
   *
   * @param fsUri FileSystem URI after any login details have been stripped
   * @param s3Region the s3 region
   * @return a new DynamoDB client
   * @throws IOException if any IO error happens
   */
  AmazonDynamoDBClient createDynamoDBClient(URI fsUri, String s3Region)
      throws IOException;

  /**
   * The default implementation for creating an AmazonDynamoDBClient.
   */
  class DefaultDynamoDBClientFactory extends Configured
      implements DynamoDBClientFactory {
    @Override
    public AmazonDynamoDBClient createDynamoDBClient(URI fsUri, String s3Region)
        throws IOException {
      assert getConf() != null : "Should have been configured before usage";
      Region region;
      try {
        region = Region.fromValue(s3Region);
      } catch (IllegalArgumentException e) {
        final String msg = "Region '" + s3Region +
            "' is invalid; should use the same region as S3 bucket";
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }
      LOG.info("Creating DynamoDBClient for fsUri {} in region {}",
          fsUri, region);

      final Configuration conf = getConf();
      final AWSCredentialsProvider credentials =
          createAWSCredentialProviderSet(fsUri, conf, fsUri);
      final ClientConfiguration awsConf =
          DefaultS3ClientFactory.createAwsConf(conf);
      AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(credentials, awsConf);

      ddb.withRegion(region.toAWSRegion());
      final String endPoint = conf.get(S3Guard.S3GUARD_DDB_ENDPOINT_KEY);
      if (StringUtils.isNotEmpty(endPoint)) {
        try {
          ddb.withEndpoint(conf.get(S3Guard.S3GUARD_DDB_ENDPOINT_KEY));
        } catch (IllegalArgumentException e) {
          final String msg = "Incorrect DynamoDB endpoint: "  + endPoint;
          LOG.error(msg, e);
          throw new IllegalArgumentException(msg, e);
        }
      }
      return ddb;
    }
  }

}

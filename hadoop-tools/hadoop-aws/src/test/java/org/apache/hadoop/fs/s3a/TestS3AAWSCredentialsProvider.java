/**
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

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link Constants#AWS_CREDENTIALS_PROVIDER} logic.
 *
 */
public class TestS3AAWSCredentialsProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AAWSCredentialsProvider.class);

  @Test
  public void testBadConfiguration() throws IOException {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER, "no.such.class");
    try {
      S3ATestUtils.createTestFileSystem(conf);
    } catch (IOException e) {
      if (!(e.getCause() instanceof ClassNotFoundException)) {
        LOG.error("Unexpected nested cause: {} in {}", e.getCause(), e, e);
        throw e;
      }
    }
  }

  static class BadCredentialsProvider implements AWSCredentialsProvider {

    @SuppressWarnings("unused")
    public BadCredentialsProvider(URI name, Configuration conf) {
    }

    @Override
    public AWSCredentials getCredentials() {
      return new BasicAWSCredentials("bad_key", "bad_secret");
    }

    @Override
    public void refresh() {
    }
  }

  @Test
  public void testBadCredentials() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER, BadCredentialsProvider.class.getName());
    try {
      S3ATestUtils.createTestFileSystem(conf);
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() != 403) {
        LOG.error("Unexpected status code: {}", e.getStatusCode(), e);
        throw e;
      }
    }
  }

  static class GoodCredentialsProvider extends AWSCredentialsProviderChain {

    @SuppressWarnings("unused")
    public GoodCredentialsProvider(URI name, Configuration conf) {
      super(new BasicAWSCredentialsProvider(conf.get(ACCESS_KEY),
          conf.get(SECRET_KEY)), new InstanceProfileCredentialsProvider());
    }
  }

  @Test
  public void testGoodProvider() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWS_CREDENTIALS_PROVIDER, GoodCredentialsProvider.class.getName());
    S3ATestUtils.createTestFileSystem(conf);
  }
}

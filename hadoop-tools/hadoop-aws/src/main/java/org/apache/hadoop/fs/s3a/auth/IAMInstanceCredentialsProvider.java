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

import java.io.Closeable;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is going to be an IAM credential provider which performs
 * async refresh for lower-latency on IO calls.
 * Initially it does not do this, simply shares the single IAM instance
 * across all instances. This makes it less expensive to declare.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IAMInstanceCredentialsProvider
    implements AWSCredentialsProvider, Closeable {

  private static final InstanceProfileCredentialsProvider INSTANCE =
      InstanceProfileCredentialsProvider.getInstance();

  public IAMInstanceCredentialsProvider() {
  }

  /**
   * Ask for the credentials.
   * as it invariably means "you aren't running on EC2"
   * @return the credentials
   */
  @Override
  public AWSCredentials getCredentials() {
    try {
      return INSTANCE.getCredentials();
    } catch (AmazonClientException e) {
      throw new NoAwsCredentialsException("IAMInstanceCredentialsProvider",
          e.getMessage(),
          e);
    }
  }

  @Override
  public void refresh() {
    INSTANCE.refresh();
  }

  @Override
  public void close() throws IOException {
    // until async, no-op.
  }
}

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

import org.apache.commons.lang.StringUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;

/**
 * Support session credentials for authenticating with AWS.
 *
 */
public class BasicSessionAWSCredentialsProvider implements AWSCredentialsProvider {
      private final String accessKey;
      private final String secretKey;
      private final String sessionToken;

      public BasicSessionAWSCredentialsProvider(String accessKey, String secretKey, String sessionToken) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
      }

      public AWSCredentials getCredentials() {
        if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey) && !StringUtils.isEmpty(sessionToken)) {
          return new BasicSessionCredentials(accessKey, secretKey, sessionToken);
        }
        throw new AmazonClientException(
            "Access key or secret or session token key is null");
      }

      public void refresh() {}

      @Override
      public String toString() {
        return getClass().getSimpleName();
      }

    }

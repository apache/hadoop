/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn.auth;

import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.utils.StringUtils;

import org.apache.hadoop.fs.cosn.Constants;

/**
 * the provider obtaining the cos credentials from the environment variables.
 */
public class EnvironmentVariableCredentialProvider
    implements COSCredentialsProvider {
  @Override
  public COSCredentials getCredentials() {
    String secretId = System.getenv(Constants.COSN_SECRET_ID_ENV);
    String secretKey = System.getenv(Constants.COSN_SECRET_KEY_ENV);

    secretId = StringUtils.trim(secretId);
    secretKey = StringUtils.trim(secretKey);

    if (!StringUtils.isNullOrEmpty(secretId)
        && !StringUtils.isNullOrEmpty(secretKey)) {
      return new BasicCOSCredentials(secretId, secretKey);
    } else {
      throw new CosClientException(
          "Unable to load COS credentials from environment variables" +
              "(COS_SECRET_ID or COS_SECRET_KEY)");
    }
  }

  @Override
  public String toString() {
    return "EnvironmentVariableCredentialProvider{}";
  }
}

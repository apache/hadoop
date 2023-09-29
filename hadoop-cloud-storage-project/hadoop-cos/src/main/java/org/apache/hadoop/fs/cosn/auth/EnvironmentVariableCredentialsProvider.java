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
import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.Constants;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * The provider obtaining the cos credentials from the environment variables.
 */
public class EnvironmentVariableCredentialsProvider
    extends AbstractCOSCredentialsProvider implements COSCredentialsProvider {

  public EnvironmentVariableCredentialsProvider(@Nullable URI uri,
                                                Configuration conf) {
    super(uri, conf);
  }

  @Override
  public COSCredentials getCredentials() {
    String secretId = System.getenv(Constants.COSN_SECRET_ID_ENV);
    String secretKey = System.getenv(Constants.COSN_SECRET_KEY_ENV);

    secretId = StringUtils.trim(secretId);
    secretKey = StringUtils.trim(secretKey);

    if (!StringUtils.isNullOrEmpty(secretId)
        && !StringUtils.isNullOrEmpty(secretKey)) {
      return new BasicCOSCredentials(secretId, secretKey);
    }

    return null;
  }

  @Override
  public void refresh() {
  }

  @Override
  public String toString() {
    return String.format("EnvironmentVariableCredentialsProvider{%s, %s}",
        Constants.COSN_SECRET_ID_ENV,
        Constants.COSN_SECRET_KEY_ENV);
  }
}

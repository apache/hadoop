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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.CosNConfigKeys;

/**
 * Get the credentials from the hadoop configuration.
 */
public class SimpleCredentialProvider implements COSCredentialsProvider {
  private String secretId;
  private String secretKey;

  public SimpleCredentialProvider(Configuration conf) {
    this.secretId = conf.get(
        CosNConfigKeys.COSN_SECRET_ID_KEY
    );
    this.secretKey = conf.get(
        CosNConfigKeys.COSN_SECRET_KEY_KEY
    );
  }

  @Override
  public COSCredentials getCredentials() {
    if (!StringUtils.isEmpty(this.secretId)
        && !StringUtils.isEmpty(this.secretKey)) {
      return new BasicCOSCredentials(this.secretId, this.secretKey);
    }
    throw new CosClientException("secret id or secret key is unset");
  }

}

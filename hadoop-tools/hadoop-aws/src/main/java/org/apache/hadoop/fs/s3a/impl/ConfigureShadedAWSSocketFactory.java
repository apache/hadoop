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

package org.apache.hadoop.fs.s3a.impl;

import javax.net.ssl.HostnameVerifier;
import java.io.IOException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.thirdparty.apache.http.conn.ssl.SSLConnectionSocketFactory;

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

/**
 * This interacts with the Shaded httpclient library used in the full
 * AWS SDK. If the S3A client is used with the unshaded SDK, this
 * class will not link.
 */
public class ConfigureShadedAWSSocketFactory implements
    NetworkBinding.ConfigureAWSSocketFactory {

  @Override
  public void configureSocketFactory(final ClientConfiguration awsConf,
      final DelegatingSSLSocketFactory.SSLChannelMode channelMode)
      throws IOException {
    DelegatingSSLSocketFactory.initializeDefaultFactory(channelMode);
    awsConf.getApacheHttpClientConfig().setSslSocketFactory(
        new SSLConnectionSocketFactory(
            DelegatingSSLSocketFactory.getDefaultFactory(),
            (HostnameVerifier) null));
  }
}

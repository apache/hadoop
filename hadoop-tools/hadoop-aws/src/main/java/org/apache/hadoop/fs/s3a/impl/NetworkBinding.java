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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;

import com.amazonaws.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SSL_CHANNEL_MODE;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SSL_CHANNEL_MODE;

/**
 * Configures network settings when communicating with AWS services.
 */
public final class NetworkBinding {

  private static final Logger LOG =
          LoggerFactory.getLogger(NetworkBinding.class);
  private static final String BINDING_CLASSNAME = "org.apache.hadoop.fs.s3a.impl.ConfigureShadedAWSSocketFactory";

  private NetworkBinding() {
  }

  /**
   * Configures the {@code SSLConnectionSocketFactory} used by the AWS SDK.
   * A custom Socket Factory can be set using the method
   * {@code setSslSocketFactory()}.
   * Uses reflection to do this via {@link ConfigureShadedAWSSocketFactory}
   * so as to avoid 
   * @param conf the {@link Configuration} used to get the client specified
   *             value of {@code SSL_CHANNEL_MODE}
   * @param awsConf the {@code ClientConfiguration} to set the
   *                SSLConnectionSocketFactory for.
   * @throws IOException if there is an error while initializing the
   * {@code SSLSocketFactory} other than classloader problems.
   */
  public static void bindSSLChannelMode(Configuration conf,
      ClientConfiguration awsConf) throws IOException {

    // Validate that SSL_CHANNEL_MODE is set to a valid value.
    String channelModeString = conf.getTrimmed(
            SSL_CHANNEL_MODE, DEFAULT_SSL_CHANNEL_MODE.name());
    DelegatingSSLSocketFactory.SSLChannelMode channelMode = null;
    for (DelegatingSSLSocketFactory.SSLChannelMode mode :
            DelegatingSSLSocketFactory.SSLChannelMode.values()) {
      if (mode.name().equalsIgnoreCase(channelModeString)) {
        channelMode = mode;
      }
    }
    if (channelMode == null) {
      throw new IllegalArgumentException(channelModeString +
              " is not a valid value for " + SSL_CHANNEL_MODE);
    }

    DelegatingSSLSocketFactory.initializeDefaultFactory(channelMode);
    try {
      // use reflection to load in our own binding class.
      // this is *probably* overkill, but it is how we can be fully confident
      // that no attempt will be made to load/link to the AWS Shaded SDK except
      // within this try/catch block
      Class<? extends ConfigureAWSSocketFactory> clazz =
          (Class<? extends ConfigureAWSSocketFactory>) Class.forName(BINDING_CLASSNAME);
      clazz.getConstructor()
          .newInstance()
          .configureSocketFactory(awsConf, channelMode);
    } catch (ClassNotFoundException | NoSuchMethodException |
            IllegalAccessException | InstantiationException |
            InvocationTargetException | LinkageError  e) {
      LOG.debug("Unable to create class {}, value of {} will be ignored",
          BINDING_CLASSNAME, SSL_CHANNEL_MODE, e);
    }
  }

  /**
   * Interface used to bind to the socket factory, allows the code which
   * works with the shaded AWS libraries to exist in their own class.
   */
  interface ConfigureAWSSocketFactory {
    void configureSocketFactory(ClientConfiguration awsConf,
        DelegatingSSLSocketFactory.SSLChannelMode channelMode)
        throws IOException;
  }

  /**
   * Given an S3 bucket region as returned by a bucket location query,
   * fix it into a form which can be used by other AWS commands.
   * <p>
   * <a href="https://forums.aws.amazon.com/thread.jspa?messageID=796829">
   * https://forums.aws.amazon.com/thread.jspa?messageID=796829</a>
   * </p>
   * See also {@code com.amazonaws.services.s3.model.Region.fromValue()}
   * for its conversion logic.
   * @param region region from S3 call.
   * @return the region to use in DDB etc.
   */
  public static String fixBucketRegion(final String region) {
    return region == null || region.equals("US")
        ? "us-east-1"
        : region;
  }

  /**
   * Log the dns address associated with s3 endpoint. If endpoint is
   * not set in the configuration, the {@code Constants#DEFAULT_ENDPOINT}
   * will be used.
   * @param conf input configuration.
   */
  public static void logDnsLookup(Configuration conf) {
    String endPoint = conf.getTrimmed(ENDPOINT, DEFAULT_ENDPOINT);
    String hostName = endPoint;
    if (!endPoint.isEmpty() && LOG.isDebugEnabled()) {
      // Updating the hostname if there is a scheme present.
      if (endPoint.contains("://")) {
        try {
          URI uri = new URI(endPoint);
          hostName = uri.getHost();
        } catch (URISyntaxException e) {
          LOG.debug("Got URISyntaxException, ignoring");
        }
      }
      LOG.debug("Bucket endpoint : {}, Hostname : {}, DNSAddress : {}",
              endPoint,
              hostName,
              NetUtils.normalizeHostName(hostName));
    }
  }
}

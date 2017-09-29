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

package org.apache.hadoop.fs.s3a;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet;
import static org.apache.hadoop.fs.s3a.S3AUtils.intOption;

/**
 * The default factory implementation, which calls the AWS SDK to configure
 * and create an {@link AmazonS3Client} that communicates with the S3 service.
 */
public class DefaultS3ClientFactory extends Configured implements
    S3ClientFactory {

  protected static final Logger LOG = S3AFileSystem.LOG;

  @Override
  public AmazonS3 createS3Client(URI name) throws IOException {
    Configuration conf = getConf();
    AWSCredentialsProvider credentials =
        createAWSCredentialProviderSet(name, conf);
    final ClientConfiguration awsConf = createAwsConf(getConf());
    AmazonS3 s3 = newAmazonS3Client(credentials, awsConf);
    return createAmazonS3Client(s3, conf, credentials, awsConf);
  }

  /**
   * Create a new {@link ClientConfiguration}.
   * @param conf The Hadoop configuration
   * @return new AWS client configuration
   */
  public static ClientConfiguration createAwsConf(Configuration conf) {
    final ClientConfiguration awsConf = new ClientConfiguration();
    initConnectionSettings(conf, awsConf);
    initProxySupport(conf, awsConf);
    initUserAgent(conf, awsConf);
    return awsConf;
  }

  /**
   * Wrapper around constructor for {@link AmazonS3} client.  Override this to
   * provide an extended version of the client
   * @param credentials credentials to use
   * @param awsConf  AWS configuration
   * @return  new AmazonS3 client
   */
  protected AmazonS3 newAmazonS3Client(
      AWSCredentialsProvider credentials, ClientConfiguration awsConf) {
    return new AmazonS3Client(credentials, awsConf);
  }

  /**
   * Initializes all AWS SDK settings related to connection management.
   *
   * @param conf Hadoop configuration
   * @param awsConf AWS SDK configuration
   */
  private static void initConnectionSettings(Configuration conf,
      ClientConfiguration awsConf) {
    awsConf.setMaxConnections(intOption(conf, MAXIMUM_CONNECTIONS,
        DEFAULT_MAXIMUM_CONNECTIONS, 1));
    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS,
        DEFAULT_SECURE_CONNECTIONS);
    awsConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);
    awsConf.setMaxErrorRetry(intOption(conf, MAX_ERROR_RETRIES,
        DEFAULT_MAX_ERROR_RETRIES, 0));
    awsConf.setConnectionTimeout(intOption(conf, ESTABLISH_TIMEOUT,
        DEFAULT_ESTABLISH_TIMEOUT, 0));
    awsConf.setSocketTimeout(intOption(conf, SOCKET_TIMEOUT,
        DEFAULT_SOCKET_TIMEOUT, 0));
    int sockSendBuffer = intOption(conf, SOCKET_SEND_BUFFER,
        DEFAULT_SOCKET_SEND_BUFFER, 2048);
    int sockRecvBuffer = intOption(conf, SOCKET_RECV_BUFFER,
        DEFAULT_SOCKET_RECV_BUFFER, 2048);
    awsConf.setSocketBufferSizeHints(sockSendBuffer, sockRecvBuffer);
    String signerOverride = conf.getTrimmed(SIGNING_ALGORITHM, "");
    if (!signerOverride.isEmpty()) {
      LOG.debug("Signer override = {}", signerOverride);
      awsConf.setSignerOverride(signerOverride);
    }
  }

  /**
   * Initializes AWS SDK proxy support if configured.
   *
   * @param conf Hadoop configuration
   * @param awsConf AWS SDK configuration
   * @throws IllegalArgumentException if misconfigured
   */
  private static void initProxySupport(Configuration conf,
      ClientConfiguration awsConf) throws IllegalArgumentException {
    String proxyHost = conf.getTrimmed(PROXY_HOST, "");
    int proxyPort = conf.getInt(PROXY_PORT, -1);
    if (!proxyHost.isEmpty()) {
      awsConf.setProxyHost(proxyHost);
      if (proxyPort >= 0) {
        awsConf.setProxyPort(proxyPort);
      } else {
        if (conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS)) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          awsConf.setProxyPort(443);
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          awsConf.setProxyPort(80);
        }
      }
      String proxyUsername = conf.getTrimmed(PROXY_USERNAME);
      String proxyPassword = conf.getTrimmed(PROXY_PASSWORD);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or " +
            PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      awsConf.setProxyUsername(proxyUsername);
      awsConf.setProxyPassword(proxyPassword);
      awsConf.setProxyDomain(conf.getTrimmed(PROXY_DOMAIN));
      awsConf.setProxyWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} with password {} on " +
                "domain {} as workstation {}", awsConf.getProxyHost(),
            awsConf.getProxyPort(),
            String.valueOf(awsConf.getProxyUsername()),
            awsConf.getProxyPassword(), awsConf.getProxyDomain(),
            awsConf.getProxyWorkstation());
      }
    } else if (proxyPort >= 0) {
      String msg =
          "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * Initializes the User-Agent header to send in HTTP requests to the S3
   * back-end.  We always include the Hadoop version number.  The user also
   * may set an optional custom prefix to put in front of the Hadoop version
   * number.  The AWS SDK interally appends its own information, which seems
   * to include the AWS SDK version, OS and JVM version.
   *
   * @param conf Hadoop configuration
   * @param awsConf AWS SDK configuration
   */
  private static void initUserAgent(Configuration conf,
      ClientConfiguration awsConf) {
    String userAgent = "Hadoop " + VersionInfo.getVersion();
    String userAgentPrefix = conf.getTrimmed(USER_AGENT_PREFIX, "");
    if (!userAgentPrefix.isEmpty()) {
      userAgent = userAgentPrefix + ", " + userAgent;
    }
    LOG.debug("Using User-Agent: {}", userAgent);
    awsConf.setUserAgentPrefix(userAgent);
  }

  /**
   * Creates an {@link AmazonS3Client} from the established configuration.
   *
   * @param conf Hadoop configuration
   * @param credentials AWS credentials
   * @param awsConf AWS SDK configuration
   * @return S3 client
   * @throws IllegalArgumentException if misconfigured
   */
  private static AmazonS3 createAmazonS3Client(AmazonS3 s3, Configuration conf,
      AWSCredentialsProvider credentials, ClientConfiguration awsConf)
      throws IllegalArgumentException {
    String endPoint = conf.getTrimmed(ENDPOINT, "");
    if (!endPoint.isEmpty()) {
      try {
        s3.setEndpoint(endPoint);
      } catch (IllegalArgumentException e) {
        String msg = "Incorrect endpoint: "  + e.getMessage();
        LOG.error(msg);
        throw new IllegalArgumentException(msg, e);
      }
    }
    enablePathStyleAccessIfRequired(s3, conf);
    return s3;
  }

  /**
   * Enables path-style access to S3 buckets if configured.  By default, the
   * behavior is to use virtual hosted-style access with URIs of the form
   * http://bucketname.s3.amazonaws.com.  Enabling path-style access and a
   * region-specific endpoint switches the behavior to use URIs of the form
   * http://s3-eu-west-1.amazonaws.com/bucketname.
   *
   * @param s3 S3 client
   * @param conf Hadoop configuration
   */
  private static void enablePathStyleAccessIfRequired(AmazonS3 s3,
      Configuration conf) {
    final boolean pathStyleAccess = conf.getBoolean(PATH_STYLE_ACCESS, false);
    if (pathStyleAccess) {
      LOG.debug("Enabling path style access!");
      s3.setS3ClientOptions(S3ClientOptions.builder()
          .setPathStyleAccess(true)
          .build());
    }
  }
}

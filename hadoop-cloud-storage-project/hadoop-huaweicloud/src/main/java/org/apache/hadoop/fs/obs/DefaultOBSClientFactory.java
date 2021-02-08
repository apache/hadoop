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

package org.apache.hadoop.fs.obs;

import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.internal.ext.ExtObsConfiguration;
import com.obs.services.model.AuthTypeEnum;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Optional;

/**
 * The default factory implementation, which calls the OBS SDK to configure and
 * create an {@link ObsClient} that communicates with the OBS service.
 */
class DefaultOBSClientFactory extends Configured implements OBSClientFactory {

  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DefaultOBSClientFactory.class);

  /**
   * Initializes all OBS SDK settings related to connection management.
   *
   * @param conf    Hadoop configuration
   * @param obsConf OBS SDK configuration
   */
  @SuppressWarnings("deprecation")
  private static void initConnectionSettings(final Configuration conf,
      final ExtObsConfiguration obsConf) {

    obsConf.setMaxConnections(
        OBSCommonUtils.intOption(conf, OBSConstants.MAXIMUM_CONNECTIONS,
            OBSConstants.DEFAULT_MAXIMUM_CONNECTIONS,
            1));

    boolean secureConnections = conf.getBoolean(
        OBSConstants.SECURE_CONNECTIONS,
        OBSConstants.DEFAULT_SECURE_CONNECTIONS);

    obsConf.setHttpsOnly(secureConnections);

    obsConf.setMaxErrorRetry(
        OBSCommonUtils.intOption(conf, OBSConstants.MAX_ERROR_RETRIES,
            OBSConstants.DEFAULT_MAX_ERROR_RETRIES, 0));

    obsConf.setConnectionTimeout(
        OBSCommonUtils.intOption(conf, OBSConstants.ESTABLISH_TIMEOUT,
            OBSConstants.DEFAULT_ESTABLISH_TIMEOUT, 0));

    obsConf.setSocketTimeout(
        OBSCommonUtils.intOption(conf, OBSConstants.SOCKET_TIMEOUT,
            OBSConstants.DEFAULT_SOCKET_TIMEOUT, 0));

    obsConf.setIdleConnectionTime(
        OBSCommonUtils.intOption(conf, OBSConstants.IDLE_CONNECTION_TIME,
            OBSConstants.DEFAULT_IDLE_CONNECTION_TIME,
            1));

    obsConf.setMaxIdleConnections(
        OBSCommonUtils.intOption(conf, OBSConstants.MAX_IDLE_CONNECTIONS,
            OBSConstants.DEFAULT_MAX_IDLE_CONNECTIONS,
            1));

    obsConf.setReadBufferSize(
        OBSCommonUtils.intOption(conf, OBSConstants.READ_BUFFER_SIZE,
            OBSConstants.DEFAULT_READ_BUFFER_SIZE,
            -1)); // to be
    // modified
    obsConf.setWriteBufferSize(
        OBSCommonUtils.intOption(conf, OBSConstants.WRITE_BUFFER_SIZE,
            OBSConstants.DEFAULT_WRITE_BUFFER_SIZE,
            -1)); // to be
    // modified
    obsConf.setUploadStreamRetryBufferSize(
        OBSCommonUtils.intOption(conf,
            OBSConstants.UPLOAD_STREAM_RETRY_SIZE,
            OBSConstants.DEFAULT_UPLOAD_STREAM_RETRY_SIZE, 1));

    obsConf.setSocketReadBufferSize(
        OBSCommonUtils.intOption(conf, OBSConstants.SOCKET_RECV_BUFFER,
            OBSConstants.DEFAULT_SOCKET_RECV_BUFFER, -1));
    obsConf.setSocketWriteBufferSize(
        OBSCommonUtils.intOption(conf, OBSConstants.SOCKET_SEND_BUFFER,
            OBSConstants.DEFAULT_SOCKET_SEND_BUFFER, -1));

    obsConf.setKeepAlive(conf.getBoolean(OBSConstants.KEEP_ALIVE,
        OBSConstants.DEFAULT_KEEP_ALIVE));
    obsConf.setValidateCertificate(
        conf.getBoolean(OBSConstants.VALIDATE_CERTIFICATE,
            OBSConstants.DEFAULT_VALIDATE_CERTIFICATE));
    obsConf.setVerifyResponseContentType(
        conf.getBoolean(OBSConstants.VERIFY_RESPONSE_CONTENT_TYPE,
            OBSConstants.DEFAULT_VERIFY_RESPONSE_CONTENT_TYPE));
    obsConf.setCname(
        conf.getBoolean(OBSConstants.CNAME, OBSConstants.DEFAULT_CNAME));
    obsConf.setIsStrictHostnameVerification(
        conf.getBoolean(OBSConstants.STRICT_HOSTNAME_VERIFICATION,
            OBSConstants.DEFAULT_STRICT_HOSTNAME_VERIFICATION));

    // sdk auth type negotiation enable
    obsConf.setAuthTypeNegotiation(
        conf.getBoolean(OBSConstants.SDK_AUTH_TYPE_NEGOTIATION_ENABLE,
            OBSConstants.DEFAULT_SDK_AUTH_TYPE_NEGOTIATION_ENABLE));
    // set SDK AUTH TYPE to OBS when auth type negotiation unenabled
    if (!obsConf.isAuthTypeNegotiation()) {
      obsConf.setAuthType(AuthTypeEnum.OBS);
    }

    // okhttp retryOnConnectionFailure switch, default set to true
    obsConf.retryOnConnectionFailureInOkhttp(
        conf.getBoolean(OBSConstants.SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE,
            OBSConstants.DEFAULT_SDK_RETRY_ON_CONNECTION_FAILURE_ENABLE));

    // sdk max retry times on unexpected end of stream exception,
    // default: -1 don't retry
    int retryTime = conf.getInt(
        OBSConstants.SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION,
        OBSConstants.DEFAULT_SDK_RETRY_TIMES_ON_UNEXPECTED_END_EXCEPTION);
    if (retryTime > 0
        && retryTime < OBSConstants.DEFAULT_MAX_SDK_CONNECTION_RETRY_TIMES
        || !obsConf.isRetryOnConnectionFailureInOkhttp() && retryTime < 0) {
      retryTime = OBSConstants.DEFAULT_MAX_SDK_CONNECTION_RETRY_TIMES;
    }
    obsConf.setMaxRetryOnUnexpectedEndException(retryTime);
  }

  /**
   * Initializes OBS SDK proxy support if configured.
   *
   * @param conf    Hadoop configuration
   * @param obsConf OBS SDK configuration
   * @throws IllegalArgumentException if misconfigured
   * @throws IOException              on any failure to initialize proxy
   */
  private static void initProxySupport(final Configuration conf,
      final ExtObsConfiguration obsConf)
      throws IllegalArgumentException, IOException {
    String proxyHost = conf.getTrimmed(OBSConstants.PROXY_HOST, "");
    int proxyPort = conf.getInt(OBSConstants.PROXY_PORT, -1);

    if (!proxyHost.isEmpty() && proxyPort < 0) {
      if (conf.getBoolean(OBSConstants.SECURE_CONNECTIONS,
          OBSConstants.DEFAULT_SECURE_CONNECTIONS)) {
        LOG.warn("Proxy host set without port. Using HTTPS default "
            + OBSConstants.DEFAULT_HTTPS_PORT);
        obsConf.getHttpProxy()
            .setProxyPort(OBSConstants.DEFAULT_HTTPS_PORT);
      } else {
        LOG.warn("Proxy host set without port. Using HTTP default "
            + OBSConstants.DEFAULT_HTTP_PORT);
        obsConf.getHttpProxy()
            .setProxyPort(OBSConstants.DEFAULT_HTTP_PORT);
      }
    }
    String proxyUsername = conf.getTrimmed(OBSConstants.PROXY_USERNAME);
    String proxyPassword = null;
    char[] proxyPass = conf.getPassword(OBSConstants.PROXY_PASSWORD);
    if (proxyPass != null) {
      proxyPassword = new String(proxyPass).trim();
    }
    if ((proxyUsername == null) != (proxyPassword == null)) {
      String msg =
          "Proxy error: " + OBSConstants.PROXY_USERNAME + " or "
              + OBSConstants.PROXY_PASSWORD
              + " set without the other.";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    obsConf.setHttpProxy(proxyHost, proxyPort, proxyUsername,
        proxyPassword);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Using proxy server {}:{} as user {} on "
              + "domain {} as workstation {}",
          obsConf.getHttpProxy().getProxyAddr(),
          obsConf.getHttpProxy().getProxyPort(),
          obsConf.getHttpProxy().getProxyUName(),
          obsConf.getHttpProxy().getDomain(),
          obsConf.getHttpProxy().getWorkstation());
    }
  }

  /**
   * Creates an {@link ObsClient} from the established configuration.
   *
   * @param conf    Hadoop configuration
   * @param obsConf ObsConfiguration
   * @param name    URL
   * @return ObsClient client
   * @throws IOException on any failure to create Huawei OBS client
   */
  private static ObsClient createHuaweiObsClient(final Configuration conf,
      final ObsConfiguration obsConf, final URI name)
      throws IOException {
    Class<?> credentialsProviderClass;
    BasicSessionCredential credentialsProvider;
    ObsClient obsClient;

    try {
      credentialsProviderClass = conf.getClass(
          OBSConstants.OBS_CREDENTIALS_PROVIDER, null);
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException(
          "From option " + OBSConstants.OBS_CREDENTIALS_PROVIDER + ' '
              + c, c);
    }

    if (credentialsProviderClass == null) {
      return createObsClientWithoutCredentialsProvider(conf, obsConf,
          name);
    }

    try {
      Constructor<?> cons =
          credentialsProviderClass.getDeclaredConstructor(URI.class,
              Configuration.class);
      credentialsProvider = (BasicSessionCredential) cons.newInstance(
          name, conf);
    } catch (NoSuchMethodException
        | SecurityException
        | IllegalAccessException
        | InstantiationException
        | InvocationTargetException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException(
          "From option " + OBSConstants.OBS_CREDENTIALS_PROVIDER + ' '
              + c, c);
    }

    String sessionToken = credentialsProvider.getSessionToken();
    String ak = credentialsProvider.getOBSAccessKeyId();
    String sk = credentialsProvider.getOBSSecretKey();
    String endPoint = conf.getTrimmed(OBSConstants.ENDPOINT, "");
    obsConf.setEndPoint(endPoint);
    if (sessionToken != null && sessionToken.length() != 0) {
      obsClient = new ObsClient(ak, sk, sessionToken, obsConf);
    } else {
      obsClient = new ObsClient(ak, sk, obsConf);
    }
    return obsClient;
  }

  private static ObsClient createObsClientWithoutCredentialsProvider(
      final Configuration conf, final ObsConfiguration obsConf,
      final URI name) throws IOException {
    ObsClient obsClient;
    OBSLoginHelper.Login creds = OBSCommonUtils.getOBSAccessKeys(name,
        conf);

    String ak = creds.getUser();
    String sk = creds.getPassword();
    String token = creds.getToken();

    String endPoint = conf.getTrimmed(OBSConstants.ENDPOINT, "");
    obsConf.setEndPoint(endPoint);

    if (!StringUtils.isEmpty(ak) || !StringUtils.isEmpty(sk)) {
      obsClient = new ObsClient(ak, sk, token, obsConf);
      return obsClient;
    }

    Class<?> securityProviderClass;
    try {
      securityProviderClass = conf.getClass(
          OBSConstants.OBS_SECURITY_PROVIDER, null);
      LOG.info("From option {} get {}",
          OBSConstants.OBS_SECURITY_PROVIDER, securityProviderClass);
    } catch (RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException(
          "From option " + OBSConstants.OBS_SECURITY_PROVIDER + ' ' + c,
          c);
    }

    if (securityProviderClass == null) {
      obsClient = new ObsClient(ak, sk, token, obsConf);
      return obsClient;
    }

    IObsCredentialsProvider securityProvider;
    try {
      Optional<Constructor> cons = tryGetConstructor(
          securityProviderClass,
          new Class[] {URI.class, Configuration.class});

      if (cons.isPresent()) {
        securityProvider = (IObsCredentialsProvider) cons.get()
            .newInstance(name, conf);
      } else {
        securityProvider
            = (IObsCredentialsProvider) securityProviderClass
            .getDeclaredConstructor().newInstance();
      }

    } catch (NoSuchMethodException
        | IllegalAccessException
        | InstantiationException
        | InvocationTargetException
        | RuntimeException e) {
      Throwable c = e.getCause() != null ? e.getCause() : e;
      throw new IOException(
          "From option " + OBSConstants.OBS_SECURITY_PROVIDER + ' ' + c,
          c);
    }
    obsClient = new ObsClient(securityProvider, obsConf);

    return obsClient;
  }

  public static Optional<Constructor> tryGetConstructor(final Class mainClss,
      final Class[] args) {
    try {
      Constructor constructor = mainClss.getDeclaredConstructor(args);
      return Optional.ofNullable(constructor);
    } catch (NoSuchMethodException e) {
      // ignore
      return Optional.empty();
    }
  }

  @Override
  public ObsClient createObsClient(final URI name) throws IOException {
    Configuration conf = getConf();
    ExtObsConfiguration obsConf = new ExtObsConfiguration();
    initConnectionSettings(conf, obsConf);
    initProxySupport(conf, obsConf);

    return createHuaweiObsClient(conf, obsConf, name);
  }
}

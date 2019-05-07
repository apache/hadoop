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

package org.apache.hadoop.hdds.scm.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;


/**
 * Utility methods for Ozone and Container Clients.
 *
 * The methods to retrieve SCM service endpoints assume there is a single
 * SCM service instance. This will change when we switch to replicated service
 * instances for redundancy.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class HddsClientUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      HddsClientUtils.class);

  private static final int NO_PORT = -1;

  private HddsClientUtils() {
  }

  /**
   * Date format that used in ozone. Here the format is thread safe to use.
   */
  private static final ThreadLocal<DateTimeFormatter> DATE_FORMAT =
      ThreadLocal.withInitial(() -> {
        DateTimeFormatter format =
            DateTimeFormatter.ofPattern(OzoneConsts.OZONE_DATE_FORMAT);
        return format.withZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));
      });


  /**
   * Convert time in millisecond to a human readable format required in ozone.
   * @return a human readable string for the input time
   */
  public static String formatDateTime(long millis) {
    ZonedDateTime dateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(millis), DATE_FORMAT.get().getZone());
    return DATE_FORMAT.get().format(dateTime);
  }

  /**
   * Convert time in ozone date format to millisecond.
   * @return time in milliseconds
   */
  public static long formatDateTime(String date) throws ParseException {
    Preconditions.checkNotNull(date, "Date string should not be null.");
    return ZonedDateTime.parse(date, DATE_FORMAT.get())
        .toInstant().toEpochMilli();
  }



  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName)
      throws IllegalArgumentException {

    if (resName == null) {
      throw new IllegalArgumentException("Bucket or Volume name is null");
    }

    if ((resName.length() < OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH) ||
        (resName.length() > OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH)) {
      throw new IllegalArgumentException(
          "Bucket or Volume length is illegal, " +
              "valid length is 3-63 characters");
    }

    if ((resName.charAt(0) == '.') || (resName.charAt(0) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot start with a period or dash");
    }

    if ((resName.charAt(resName.length() - 1) == '.') ||
        (resName.charAt(resName.length() - 1) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot end with a period or dash");
    }

    boolean isIPv4 = true;
    char prev = (char) 0;

    for (int index = 0; index < resName.length(); index++) {
      char currChar = resName.charAt(index);

      if (currChar != '.') {
        isIPv4 = ((currChar >= '0') && (currChar <= '9')) && isIPv4;
      }

      if (currChar > 'A' && currChar < 'Z') {
        throw new IllegalArgumentException(
            "Bucket or Volume name does not support uppercase characters");
      }

      if ((currChar != '.') && (currChar != '-')) {
        if ((currChar < '0') || (currChar > '9' && currChar < 'a') ||
            (currChar > 'z')) {
          throw new IllegalArgumentException("Bucket or Volume name has an " +
              "unsupported character : " +
              currChar);
        }
      }

      if ((prev == '.') && (currChar == '.')) {
        throw new IllegalArgumentException("Bucket or Volume name should not " +
            "have two contiguous periods");
      }

      if ((prev == '-') && (currChar == '.')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have period after dash");
      }

      if ((prev == '.') && (currChar == '-')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have dash after period");
      }
      prev = currChar;
    }

    if (isIPv4) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot be an IPv4 address or all numeric");
    }
  }

  /**
   * verifies that bucket / volume name is a valid DNS name.
   *
   * @param resourceNames Array of bucket / volume names to be verified.
   */
  public static void verifyResourceName(String... resourceNames) {
    for (String resourceName : resourceNames) {
      HddsClientUtils.verifyResourceName(resourceName);
    }
  }

  /**
   * Checks that object parameters passed as reference is not null.
   *
   * @param references Array of object references to be checked.
   * @param <T>
   */
  public static <T> void checkNotNull(T... references) {
    for (T ref: references) {
      Preconditions.checkNotNull(ref);
    }
  }

  /**
   * Returns the cache value to be used for list calls.
   * @param conf Configuration object
   * @return list cache size
   */
  public static int getListCacheSize(Configuration conf) {
    return conf.getInt(OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE,
        OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE_DEFAULT);
  }

  /**
   * @return a default instance of {@link CloseableHttpClient}.
   */
  public static CloseableHttpClient newHttpClient() {
    return HddsClientUtils.newHttpClient(new Configuration());
  }

  /**
   * Returns a {@link CloseableHttpClient} configured by given configuration.
   * If conf is null, returns a default instance.
   *
   * @param conf configuration
   * @return a {@link CloseableHttpClient} instance.
   */
  public static CloseableHttpClient newHttpClient(Configuration conf) {
    long socketTimeout = OzoneConfigKeys
        .OZONE_CLIENT_SOCKET_TIMEOUT_DEFAULT;
    long connectionTimeout = OzoneConfigKeys
        .OZONE_CLIENT_CONNECTION_TIMEOUT_DEFAULT;
    if (conf != null) {
      socketTimeout = conf.getTimeDuration(
          OzoneConfigKeys.OZONE_CLIENT_SOCKET_TIMEOUT,
          OzoneConfigKeys.OZONE_CLIENT_SOCKET_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      connectionTimeout = conf.getTimeDuration(
          OzoneConfigKeys.OZONE_CLIENT_CONNECTION_TIMEOUT,
          OzoneConfigKeys.OZONE_CLIENT_CONNECTION_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
    }

    CloseableHttpClient client = HttpClients.custom()
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setSocketTimeout(Math.toIntExact(socketTimeout))
                .setConnectTimeout(Math.toIntExact(connectionTimeout))
                .build())
        .build();
    return client;
  }

  /**
   * Returns the maximum no of outstanding async requests to be handled by
   * Standalone and Ratis client.
   */
  public static int getMaxOutstandingRequests(Configuration config) {
    return config
        .getInt(ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_OUTSTANDING_REQUESTS,
            ScmConfigKeys
                .SCM_CONTAINER_CLIENT_MAX_OUTSTANDING_REQUESTS_DEFAULT);
  }

  /**
   * Create a scm block client, used by putKey() and getKey().
   *
   * @return {@link ScmBlockLocationProtocol}
   * @throws IOException
   */
  public static SCMSecurityProtocol getScmSecurityClient(
      OzoneConfiguration conf, UserGroupInformation ugi) throws IOException {
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress scmSecurityProtoAdd =
        HddsUtils.getScmAddressForSecurityProtocol(conf);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        new SCMSecurityProtocolClientSideTranslatorPB(
            RPC.getProxy(SCMSecurityProtocolPB.class, scmVersion,
                scmSecurityProtoAdd, ugi, conf,
                NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));
    return scmSecurityClient;
  }
}
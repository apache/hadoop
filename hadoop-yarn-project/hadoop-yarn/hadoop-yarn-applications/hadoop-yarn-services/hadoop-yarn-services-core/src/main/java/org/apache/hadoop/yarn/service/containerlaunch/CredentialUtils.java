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

package org.apache.hadoop.yarn.service.containerlaunch;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;

/**
 * Utils to work with credentials and tokens.
 *
 * Designed to be movable to Hadoop core
 */
public final class CredentialUtils {

  private CredentialUtils() {
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(CredentialUtils.class);

  /**
   * Save credentials to a byte buffer. Returns null if there were no
   * credentials to save
   * @param credentials credential set
   * @return a byte buffer of serialized tokens
   * @throws IOException if the credentials could not be written to the stream
   */
  public static ByteBuffer marshallCredentials(Credentials credentials) throws IOException {
    ByteBuffer buffer = null;
    if (!credentials.getAllTokens().isEmpty()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      try {
        credentials.writeTokenStorageToStream(dob);
      } finally {
        dob.close();
      }
      buffer = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }
    return buffer;
  }

  /**
   * Save credentials to a file
   * @param file file to save to (will be overwritten)
   * @param credentials credentials to write
   * @throws IOException
   */
  public static void saveTokens(File file,
      Credentials credentials) throws IOException {
    try(DataOutputStream daos = new DataOutputStream(
        new FileOutputStream(file))) {
      credentials.writeTokenStorageToStream(daos);
    }
  }

  /**
   * Look up and return the resource manager's principal. This method
   * automatically does the <code>_HOST</code> replacement in the principal and
   * correctly handles HA resource manager configurations.
   *
   * From: YARN-4629
   * @param conf the {@link Configuration} file from which to read the
   * principal
   * @return the resource manager's principal string
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRMPrincipal(Configuration conf) throws IOException {
    String principal = conf.get(RM_PRINCIPAL, "");
    String hostname;
    Preconditions.checkState(!principal.isEmpty(), "Not set: " + RM_PRINCIPAL);

    if (HAUtil.isHAEnabled(conf)) {
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      if (yarnConf.get(RM_HA_ID) == null) {
        // If RM_HA_ID is not configured, use the first of RM_HA_IDS.
        // Any valid RM HA ID should work.
        String[] rmIds = yarnConf.getStrings(RM_HA_IDS);
        Preconditions.checkState((rmIds != null) && (rmIds.length > 0),
            "Not set " + RM_HA_IDS);
        yarnConf.set(RM_HA_ID, rmIds[0]);
      }

      hostname = yarnConf.getSocketAddr(
          RM_ADDRESS,
          DEFAULT_RM_ADDRESS,
          DEFAULT_RM_PORT).getHostName();
    } else {
      hostname = conf.getSocketAddr(
          RM_ADDRESS,
          DEFAULT_RM_ADDRESS,
          DEFAULT_RM_PORT).getHostName();
    }
    return SecurityUtil.getServerPrincipal(principal, hostname);
  }

  /**
   * Create and add any filesystem delegation tokens with
   * the RM(s) configured to be able to renew them. Returns null
   * on an insecure cluster (i.e. harmless)
   * @param conf configuration
   * @param fs filesystem
   * @param credentials credentials to update
   * @return a list of all added tokens.
   * @throws IOException
   */
  public static Token<?>[] addRMRenewableFSDelegationTokens(Configuration conf,
      FileSystem fs,
      Credentials credentials) throws IOException {
    Preconditions.checkArgument(conf != null);
    Preconditions.checkArgument(credentials != null);
    if (UserGroupInformation.isSecurityEnabled()) {
      return fs.addDelegationTokens(CredentialUtils.getRMPrincipal(conf),
          credentials);
    }
    return null;
  }

  /**
   * Add an FS delegation token which can be renewed by the current user
   * @param fs filesystem
   * @param credentials credentials to update
   * @throws IOException problems.
   */
  public static void addSelfRenewableFSDelegationTokens(
      FileSystem fs,
      Credentials credentials) throws IOException {
    Preconditions.checkArgument(fs != null);
    Preconditions.checkArgument(credentials != null);
    fs.addDelegationTokens(
        getSelfRenewer(),
        credentials);
  }

  public static String getSelfRenewer() throws IOException {
    return UserGroupInformation.getLoginUser().getShortUserName();
  }

  /**
   * Create and add an RM delegation token to the credentials
   * @param yarnClient Yarn Client
   * @param credentials to add token to
   * @return the token which was added
   * @throws IOException
   * @throws YarnException
   */
  public static Token<TokenIdentifier> addRMDelegationToken(YarnClient yarnClient,
      Credentials credentials)
      throws IOException, YarnException {
    Configuration conf = yarnClient.getConfig();
    Text rmPrincipal = new Text(CredentialUtils.getRMPrincipal(conf));
    Text rmDTService = ClientRMProxy.getRMDelegationTokenService(conf);
    Token<TokenIdentifier> rmDelegationToken =
        ConverterUtils.convertFromYarn(
            yarnClient.getRMDelegationToken(rmPrincipal),
            rmDTService);
    credentials.addToken(rmDelegationToken.getService(), rmDelegationToken);
    return rmDelegationToken;
  }

  public static Token<TimelineDelegationTokenIdentifier> maybeAddTimelineToken(
      Configuration conf,
      Credentials credentials)
      throws IOException, YarnException {
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false)) {
      LOG.debug("Timeline service enabled -fetching token");

      try(TimelineClient timelineClient = TimelineClient.createTimelineClient()) {
        timelineClient.init(conf);
        timelineClient.start();
        Token<TimelineDelegationTokenIdentifier> token =
            timelineClient.getDelegationToken(
                CredentialUtils.getRMPrincipal(conf));
        credentials.addToken(token.getService(), token);
        return token;
      }
    } else {
      LOG.debug("Timeline service is disabled");
      return null;
    }
  }

  /**
   * Filter a list of tokens from a set of credentials
   * @param credentials credential source (a new credential set os re
   * @param filter List of tokens to strip out
   * @return a new, filtered, set of credentials
   */
  public static Credentials filterTokens(Credentials credentials,
      List<Text> filter) {
    Credentials result = new Credentials(credentials);
    Iterator<Token<? extends TokenIdentifier>> iter =
        result.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<? extends TokenIdentifier> token = iter.next();
      LOG.debug("Token {}", token.getKind());
      if (filter.contains(token.getKind())) {
        LOG.debug("Filtering token {}", token.getKind());
        iter.remove();
      }
    }
    return result;
  }

  public static String dumpTokens(Credentials credentials, String separator) {
    ArrayList<Token<? extends TokenIdentifier>> sorted =
        new ArrayList<>(credentials.getAllTokens());
    Collections.sort(sorted, new TokenComparator());
    StringBuilder buffer = new StringBuilder(sorted.size()* 128);
    for (Token<? extends TokenIdentifier> token : sorted) {
      buffer.append(tokenToString(token)).append(separator);
    }
    return buffer.toString();
  }

  /**
   * Create a string for people to look at
   * @param token token to convert to a string form
   * @return a printable view of the token
   */
  public static String tokenToString(Token<? extends TokenIdentifier> token) {
    DateFormat df = DateFormat.getDateTimeInstance(
        DateFormat.SHORT, DateFormat.SHORT);
    StringBuilder buffer = new StringBuilder(128);
    buffer.append(token.toString());
    try {
      TokenIdentifier ti = token.decodeIdentifier();
      buffer.append("; ").append(ti);
      if (ti instanceof AbstractDelegationTokenIdentifier) {
        // details in human readable form, and compensate for information HDFS DT omits
        AbstractDelegationTokenIdentifier dt = (AbstractDelegationTokenIdentifier) ti;
        buffer.append("; Renewer: ").append(dt.getRenewer());
        buffer.append("; Issued: ")
            .append(df.format(new Date(dt.getIssueDate())));
        buffer.append("; Max Date: ")
            .append(df.format(new Date(dt.getMaxDate())));
      }
    } catch (IOException e) {
      //marshall problem; not ours
      LOG.debug("Failed to decode {}: {}", token, e, e);
    }
    return buffer.toString();
  }

  /**
   * Get the expiry time of a token.
   * @param token token to examine
   * @return the time in milliseconds after which the token is invalid.
   * @throws IOException
   */
  public static long getTokenExpiryTime(Token token) throws IOException {
    TokenIdentifier identifier = token.decodeIdentifier();
    Preconditions.checkState(identifier instanceof AbstractDelegationTokenIdentifier,
        "Token %s of type: %s has an identifier which cannot be examined: %s",
        token, token.getClass(), identifier);
    AbstractDelegationTokenIdentifier id =
        (AbstractDelegationTokenIdentifier) identifier;
    return id.getMaxDate();
  }

  private static class TokenComparator
      implements Comparator<Token<? extends TokenIdentifier>>, Serializable {
    @Override
    public int compare(Token<? extends TokenIdentifier> left,
        Token<? extends TokenIdentifier> right) {
      return left.getKind().toString().compareTo(right.getKind().toString());
    }
  }
}

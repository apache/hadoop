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

package org.apache.hadoop.yarn.server.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that contains commonly used server methods.
 *
 */
@Private
public final class YarnServerSecurityUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnServerSecurityUtils.class);

  private static final String CCJ_FIPS_APPROVED_ONLY_PROPERTY =
      "com.safelogic.cryptocomply.fips.approved_only";

  private YarnServerSecurityUtils() {
  }

  /**
   * Authorizes the current request and returns the AMRMTokenIdentifier for the
   * current application.
   *
   * @return the AMRMTokenIdentifier instance for the current user
   * @throws YarnException exceptions from yarn servers.
   */
  public static AMRMTokenIdentifier authorizeRequest() throws YarnException {

    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg =
          "Cannot obtain the user-name for authorizing ApplicationMaster. "
              + "Got exception: " + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }

    boolean tokenFound = false;
    String message = "";
    AMRMTokenIdentifier appTokenIdentifier = null;
    try {
      appTokenIdentifier = selectAMRMTokenIdentifier(remoteUgi);
      if (appTokenIdentifier == null) {
        tokenFound = false;
        message = "No AMRMToken found for user " + remoteUgi.getUserName();
      } else {
        tokenFound = true;
      }
    } catch (IOException e) {
      tokenFound = false;
      message = "Got exception while looking for AMRMToken for user "
          + remoteUgi.getUserName();
    }

    if (!tokenFound) {
      LOG.warn(message);
      throw RPCUtil.getRemoteException(message);
    }

    return appTokenIdentifier;
  }

  // Obtain the needed AMRMTokenIdentifier from the remote-UGI. RPC layer
  // currently sets only the required id, but iterate through anyways just to be
  // sure.
  private static AMRMTokenIdentifier selectAMRMTokenIdentifier(
      UserGroupInformation remoteUgi) throws IOException {
    AMRMTokenIdentifier result = null;
    Set<TokenIdentifier> tokenIds = remoteUgi.getTokenIdentifiers();
    for (TokenIdentifier tokenId : tokenIds) {
      if (tokenId instanceof AMRMTokenIdentifier) {
        result = (AMRMTokenIdentifier) tokenId;
        break;
      }
    }

    return result;
  }

  /**
   * Update the new AMRMToken into the ugi used for RM proxy.
   *
   * @param token the new AMRMToken sent by RM
   * @param user ugi used for RM proxy
   * @param conf configuration
   */
  public static void updateAMRMToken(
      org.apache.hadoop.yarn.api.records.Token token, UserGroupInformation user,
      Configuration conf) {
    Token<AMRMTokenIdentifier> amrmToken = new Token<AMRMTokenIdentifier>(
        token.getIdentifier().array(), token.getPassword().array(),
        new Text(token.getKind()), new Text(token.getService()));
    // Preserve the token service sent by the RM when adding the token
    // to ensure we replace the previous token setup by the RM.
    // Afterwards we can update the service address for the RPC layer.
    user.addToken(amrmToken);
    amrmToken.setService(ClientRMProxy.getAMRMTokenService(conf));
  }

  /**
   * Parses the container launch context and returns a Credential instance that
   * contains all the tokens from the launch context.
   *
   * @param launchContext ContainerLaunchContext.
   * @return the credential instance
   * @throws IOException if there are I/O errors.
   */
  public static Credentials parseCredentials(
      ContainerLaunchContext launchContext) throws IOException {
    Credentials credentials = new Credentials();
    ByteBuffer tokens = launchContext.getTokens();

    if (tokens != null) {
      DataInputByteBuffer buf = new DataInputByteBuffer();
      tokens.rewind();
      buf.reset(tokens);
      credentials.readTokenStorageStream(buf);
      if (LOG.isDebugEnabled()) {
        for (Token<? extends TokenIdentifier> tk : credentials.getAllTokens()) {
          LOG.debug("{}={}", tk.getService(), tk);
        }
      }
    }

    return credentials;
  }
  
  public static boolean isFipsEnabled() {
    String fipsApprovedModeValue = System.getProperty(
        CCJ_FIPS_APPROVED_ONLY_PROPERTY);
    final boolean fipsEnabled = fipsApprovedModeValue != null &&
        Boolean.valueOf(fipsApprovedModeValue);
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("FIPS mode: {}, value of JVM property '{}' is: {}",
          fipsEnabled, CCJ_FIPS_APPROVED_ONLY_PROPERTY, fipsApprovedModeValue);
    }
    return fipsEnabled;
  }

  public static void printSecurityProviders() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Available Security Providers are:");
    }
    
    Provider[] providers = Security.getProviders();
    for (int i = 0; i < providers.length; i++) {
      Provider provider = providers[i];
      if (LOG.isTraceEnabled()) {
        LOG.trace("[" + (i + 1) + "] - Name: " + provider.getName());
        LOG.trace("Information:\n" + provider.getInfo());
        LOG.trace("Listing providers with types of service " +
            "and algorithm provided:\n");
      }

      Set<Provider.Service> services = provider.getServices();
      List<Provider.Service> servicesList = new ArrayList<>(services);
      servicesList.sort(Comparator.comparing(Provider.Service::getType));
      for (Provider.Service service : servicesList) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("- Name: %s, Service Type: %s, Algorithm: %s",
              provider.getName(), service.getType(), service.getAlgorithm()));
        }
      }
    }
  }

  /**
   * Return the index af the given Security Provider.
   * Note that the index is 1-based.
   * @param providerName
   * @return The index, or -1 if the provider is null or was not found.
   */
  public static int getProviderIndex(String providerName) {
    int providerIndex = 1;
    if (providerName == null) {
      return -1;
    }
    for (Provider provider : Security.getProviders()) {
      if (providerName.equalsIgnoreCase(provider.getName())) {
        return providerIndex;
      }
      providerIndex++;
    }
    return -1;
  }
}

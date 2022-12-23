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

  private YarnServerSecurityUtils() {
  }

  /**
   * Authorizes the current request and returns the AMRMTokenIdentifier for the
   * current application.
   *
   * @return the AMRMTokenIdentifier instance for the current user
   * @throws YarnException
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
   * @param launchContext
   * @return the credential instance
   * @throws IOException
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
}

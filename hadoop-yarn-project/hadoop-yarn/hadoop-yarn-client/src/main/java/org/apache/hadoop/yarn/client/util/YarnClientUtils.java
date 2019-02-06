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
package org.apache.hadoop.yarn.client.util;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a container for utility methods that are useful when creating
 * YARN clients.
 */
public abstract class YarnClientUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(YarnClientUtils.class);
  private static final Base64 BASE_64_CODEC = new Base64(0);
  private static final String ADD_LABEL_FORMAT_ERR_MSG =
      "Input format for adding node-labels is not correct, it should be "
          + "labelName1[(exclusive=true/false)],LabelName2[] ..";

  public static final String NO_LABEL_ERR_MSG =
      "No cluster node-labels are specified";

  /**
   * Look up and return the resource manager's principal. This method
   * automatically does the <code>_HOST</code> replacement in the principal and
   * correctly handles HA resource manager configurations.
   *
   * @param conf the {@link Configuration} file from which to read the
   * principal
   * @return the resource manager's principal string or null if the
   * {@link YarnConfiguration#RM_PRINCIPAL} property is not set in the
   * {@code conf} parameter
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRmPrincipal(Configuration conf) throws IOException {
    String principal = conf.get(YarnConfiguration.RM_PRINCIPAL);
    String prepared = null;

    if (principal != null) {
      prepared = getRmPrincipal(principal, conf);
    }

    return prepared;
  }

  /**
   * Perform the <code>_HOST</code> replacement in the {@code principal},
   * Returning the result. Correctly handles HA resource manager configurations.
   *
   * @param rmPrincipal the principal string to prepare
   * @param conf the configuration
   * @return the prepared principal string
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRmPrincipal(String rmPrincipal, Configuration conf)
      throws IOException {
    if (rmPrincipal == null) {
      throw new IllegalArgumentException("RM principal string is null");
    }

    if (HAUtil.isHAEnabled(conf)) {
      conf = getYarnConfWithRmHaId(conf);
    }

    String hostname = conf.getSocketAddr(
        YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT).getHostName();

    return SecurityUtil.getServerPrincipal(rmPrincipal, hostname);
  }

  /**
   * Creates node labels from string
   * @param args nodelabels string to be parsed
   * @return list of node labels
   */
  public static List<NodeLabel> buildNodeLabelsFromStr(String args) {
    List<NodeLabel> nodeLabels = new ArrayList<>();
    for (String p : args.split(",")) {
      if (!p.trim().isEmpty()) {
        String labelName = p;

        // Try to parse exclusive
        boolean exclusive = NodeLabel.DEFAULT_NODE_LABEL_EXCLUSIVITY;
        int leftParenthesisIdx = p.indexOf("(");
        int rightParenthesisIdx = p.indexOf(")");

        if ((leftParenthesisIdx == -1 && rightParenthesisIdx != -1)
            || (leftParenthesisIdx != -1 && rightParenthesisIdx == -1)) {
          // Parentheses not match
          throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
        }

        if (leftParenthesisIdx > 0 && rightParenthesisIdx > 0) {
          if (leftParenthesisIdx > rightParenthesisIdx) {
            // Parentheses not match
            throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
          }

          String property = p.substring(p.indexOf("(") + 1, p.indexOf(")"));
          if (property.contains("=")) {
            String key = property.substring(0, property.indexOf("=")).trim();
            String value =
                property
                    .substring(property.indexOf("=") + 1, property.length())
                    .trim();

            // Now we only support one property, which is exclusive, so check if
            // key = exclusive and value = {true/false}
            if (key.equals("exclusive")
                && ImmutableSet.of("true", "false").contains(value)) {
              exclusive = Boolean.parseBoolean(value);
            } else {
              throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
            }
          } else if (!property.trim().isEmpty()) {
            throw new IllegalArgumentException(ADD_LABEL_FORMAT_ERR_MSG);
          }
        }

        // Try to get labelName if there's "(..)"
        if (labelName.contains("(")) {
          labelName = labelName.substring(0, labelName.indexOf("(")).trim();
        }

        nodeLabels.add(NodeLabel.newInstance(labelName, exclusive));
      }
    }

    if (nodeLabels.isEmpty()) {
      throw new IllegalArgumentException(NO_LABEL_ERR_MSG);
    }
    return nodeLabels;
  }

  /**
   * Returns a {@link YarnConfiguration} built from the {@code conf} parameter
   * that is guaranteed to have the {@link YarnConfiguration#RM_HA_ID}
   * property set.
   *
   * @param conf the base configuration
   * @return a {@link YarnConfiguration} built from the base
   * {@link Configuration}
   * @throws IOException thrown if the {@code conf} parameter contains
   * inconsistent properties
   */
  @VisibleForTesting
  static YarnConfiguration getYarnConfWithRmHaId(Configuration conf)
      throws IOException {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);

    if (yarnConf.get(YarnConfiguration.RM_HA_ID) == null) {
      // If RM_HA_ID is not configured, use the first of RM_HA_IDS.
      // Any valid RM HA ID should work.
      String[] rmIds = yarnConf.getStrings(YarnConfiguration.RM_HA_IDS);

      if ((rmIds != null) && (rmIds.length > 0)) {
        yarnConf.set(YarnConfiguration.RM_HA_ID, rmIds[0]);
      } else {
        throw new IOException("RM_HA_IDS property is not set for HA resource "
            + "manager");
      }
    }

    return yarnConf;
  }

  /**
   * Generate SPNEGO challenge request token.
   *
   * @param server - hostname to contact
   * @throws IOException thrown if doAs failed
   * @throws InterruptedException thrown if doAs is interrupted
   * @return SPNEGO token challenge
   */
  public static String generateToken(String server) throws IOException,
      InterruptedException {
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    LOG.debug("The user credential is {}", currentUser);
    String challenge = currentUser
        .doAs(new PrivilegedExceptionAction<String>() {
          @Override
          public String run() throws Exception {
            try {
              // This Oid for Kerberos GSS-API mechanism.
              Oid mechOid = KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID");
              GSSManager manager = GSSManager.getInstance();
              // GSS name for server
              GSSName serverName = manager.createName("HTTP@" + server,
                  GSSName.NT_HOSTBASED_SERVICE);
              // Create a GSSContext for authentication with the service.
              // We're passing client credentials as null since we want them to
              // be read from the Subject.
              GSSContext gssContext = manager.createContext(
                  serverName.canonicalize(mechOid), mechOid, null,
                  GSSContext.DEFAULT_LIFETIME);
              gssContext.requestMutualAuth(true);
              gssContext.requestCredDeleg(true);
              // Establish context
              byte[] inToken = new byte[0];
              byte[] outToken = gssContext.initSecContext(inToken, 0,
                  inToken.length);
              gssContext.dispose();
              // Base64 encoded and stringified token for server
              LOG.debug("Got valid challenge for host {}", serverName);
              return new String(BASE_64_CODEC.encode(outToken),
                  StandardCharsets.US_ASCII);
            } catch (GSSException | IllegalAccessException
                | NoSuchFieldException | ClassNotFoundException e) {
              LOG.error("Error: {}", e);
              throw new AuthenticationException(e);
            }
          }
        });
    return challenge;
  }
}

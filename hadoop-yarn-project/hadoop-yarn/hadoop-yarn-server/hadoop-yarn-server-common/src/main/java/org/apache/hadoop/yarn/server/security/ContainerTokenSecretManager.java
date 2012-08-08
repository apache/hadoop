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

package org.apache.hadoop.yarn.server.security;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.BuilderUtils;

/**
 * SecretManager for ContainerTokens. Used by both RM and NM and hence is
 * present in yarn-server-common package.
 * 
 */
public class ContainerTokenSecretManager extends
    SecretManager<ContainerTokenIdentifier> {

  private static Log LOG = LogFactory
      .getLog(ContainerTokenSecretManager.class);

  Map<String, SecretKey> secretkeys =
    new ConcurrentHashMap<String, SecretKey>();

  private final long containerTokenExpiryInterval;

  public ContainerTokenSecretManager(Configuration conf) {
    this.containerTokenExpiryInterval =
        conf.getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
  }

  public ContainerToken createContainerToken(ContainerId containerId,
      NodeId nodeId, Resource capability) {
    try {
      long expiryTimeStamp =
          System.currentTimeMillis() + containerTokenExpiryInterval;
      ContainerTokenIdentifier tokenIdentifier =
          new ContainerTokenIdentifier(containerId, nodeId.toString(),
            capability, expiryTimeStamp);
      return BuilderUtils.newContainerToken(nodeId,
        ByteBuffer.wrap(this.createPassword(tokenIdentifier)), tokenIdentifier);
    } catch (IllegalArgumentException e) {
      // this could be because DNS is down - in which case we just want
      // to retry and not bring RM down. Caller should note and act on the fact
      // that container is not creatable.
      LOG.error("Error trying to create new container", e);
      return null;
    }
  }

  // Used by master for generation of secretyKey per host
  public SecretKey createAndGetSecretKey(CharSequence hostName) {
    String hostNameStr = hostName.toString();
    if (!this.secretkeys.containsKey(hostNameStr)) {
      LOG.debug("Creating secretKey for NM " + hostNameStr);
      this.secretkeys.put(hostNameStr,
          createSecretKey("mySecretKey".getBytes()));
    }
    return this.secretkeys.get(hostNameStr);
  }

  // Used by slave for using secretKey sent by the master.
  public void setSecretKey(CharSequence hostName, byte[] secretKeyBytes) {
    this.secretkeys.put(hostName.toString(), createSecretKey(secretKeyBytes));
  }

  @Override
  public byte[] createPassword(ContainerTokenIdentifier identifier) {
    LOG.debug("Creating password for " + identifier.getContainerID()
        + " to be run on NM " + identifier.getNmHostAddress() + " "
        + this.secretkeys.get(identifier.getNmHostAddress()));
    return createPassword(identifier.getBytes(),
        this.secretkeys.get(identifier.getNmHostAddress()));
  }

  @Override
  public byte[] retrievePassword(ContainerTokenIdentifier identifier)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    LOG.debug("Retrieving password for " + identifier.getContainerID()
        + " to be run on NM " + identifier.getNmHostAddress());
    return createPassword(identifier.getBytes(),
        this.secretkeys.get(identifier.getNmHostAddress()));
  }

  @Override
  public ContainerTokenIdentifier createIdentifier() {
    return new ContainerTokenIdentifier();
  }
}

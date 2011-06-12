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

import java.util.HashMap;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;

public class ContainerTokenSecretManager extends
    SecretManager<ContainerTokenIdentifier> {

  private static Log LOG = LogFactory
      .getLog(ContainerTokenSecretManager.class);

  private Map<String, SecretKey> secretkeys =
      new HashMap<String, SecretKey>();

  // Used by master for generation of secretyKey per host
  public SecretKey createAndGetSecretKey(CharSequence hostName) {
    String hostNameStr = hostName.toString();
    if (!this.secretkeys.containsKey(hostNameStr)) {
      LOG.info("Creating secretKey for NM " + hostNameStr);
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
    LOG.info("Creating password for " + identifier.getContainerID()
        + " to be run on NM " + identifier.getNmHostName() + " ======= " + this.secretkeys.get(identifier.getNmHostName()));
    return createPassword(identifier.getBytes(),
        this.secretkeys.get(identifier.getNmHostName()));
  }

  @Override
  public byte[] retrievePassword(ContainerTokenIdentifier identifier)
      throws org.apache.hadoop.security.token.SecretManager.InvalidToken {
    LOG.info("Retrieving password for " + identifier.getContainerID()
        + " to be run on NM " + identifier.getNmHostName());
    return createPassword(identifier.getBytes(),
        this.secretkeys.get(identifier.getNmHostName()));
  }

  @Override
  public ContainerTokenIdentifier createIdentifier() {
    return new ContainerTokenIdentifier();
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.server;


import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdds.HddsConfigKeys.*;

/**
 * {@link PolicyProvider} for SCM protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class SCMPolicyProvider extends PolicyProvider {

  private static AtomicReference<SCMPolicyProvider> atomicReference =
      new AtomicReference<>();

  private SCMPolicyProvider() {
  }

  @Private
  @Unstable
  public static SCMPolicyProvider getInstance() {
    if (atomicReference.get() == null) {
      atomicReference.compareAndSet(null, new SCMPolicyProvider());
    }
    return atomicReference.get();
  }

  private static final Service[] SCM_SERVICES =
      new Service[]{
          new Service(
              HDDS_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL,
              StorageContainerDatanodeProtocol.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_CONTAINER_PROTOCOL_ACL,
              StorageContainerLocationProtocol.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_BLOCK_PROTOCOL_ACL,
              ScmBlockLocationProtocol.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_CERTIFICATE_PROTOCOL_ACL,
              SCMSecurityProtocol.class),
      };

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Override
  public Service[] getServices() {
    return SCM_SERVICES;
  }

}

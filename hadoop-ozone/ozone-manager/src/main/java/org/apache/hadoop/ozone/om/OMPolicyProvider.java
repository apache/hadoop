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
package org.apache.hadoop.ozone.om;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.ozone.om.OMConfigKeys
    .OZONE_OM_SECURITY_CLIENT_PROTOCOL_ACL;

/**
 * {@link PolicyProvider} for OM protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class OMPolicyProvider extends PolicyProvider {

  private static AtomicReference<OMPolicyProvider> atomicReference =
      new AtomicReference<>();

  private OMPolicyProvider() {
  }

  @Private
  @Unstable
  public static OMPolicyProvider getInstance() {
    if (atomicReference.get() == null) {
      atomicReference.compareAndSet(null, new OMPolicyProvider());
    }
    return atomicReference.get();
  }

  private static final Service[] OM_SERVICES =
      new Service[]{
          new Service(OZONE_OM_SECURITY_CLIENT_PROTOCOL_ACL,
              OzoneManagerProtocol.class),
      };

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Override
  public Service[] getServices() {
    return OM_SERVICES;
  }

}

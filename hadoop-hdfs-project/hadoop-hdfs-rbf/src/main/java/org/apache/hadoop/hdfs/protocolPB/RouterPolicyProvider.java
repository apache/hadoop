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
package org.apache.hadoop.hdfs.protocolPB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.security.authorize.Service;

/**
 * {@link HDFSPolicyProvider} for RBF protocols.
 */
@InterfaceAudience.Private
public class RouterPolicyProvider extends HDFSPolicyProvider {

  private static final Service[] RBF_SERVICES = new Service[] {
      new Service(CommonConfigurationKeys.SECURITY_ROUTER_ADMIN_PROTOCOL_ACL,
          RouterAdminProtocol.class) };

  private final Service[] services;

  public RouterPolicyProvider() {
    List<Service> list = new ArrayList<>();
    list.addAll(Arrays.asList(super.getServices()));
    list.addAll(Arrays.asList(RBF_SERVICES));
    services = list.toArray(new Service[list.size()]);
  }

  @Override
  public Service[] getServices() {
    return Arrays.copyOf(services, services.length);
  }
}
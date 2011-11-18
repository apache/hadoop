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
package org.apache.hadoop.hbase.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

/**
 * Implementation of secure Hadoop policy provider for mapping
 * protocol interfaces to hbase-policy.xml entries.
 */
public class HBasePolicyProvider extends PolicyProvider {
  protected static Service[] services = {
      new Service("security.client.protocol.acl", HRegionInterface.class),
      new Service("security.admin.protocol.acl", HMasterInterface.class),
      new Service("security.masterregion.protocol.acl", HMasterRegionInterface.class)
  };

  @Override
  public Service[] getServices() {
    return services;
  }

  public static void init(Configuration conf,
      ServiceAuthorizationManager authManager) {
    // set service-level authorization security policy
    conf.set("hadoop.policy.file", "hbase-policy.xml");
    if (conf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      authManager.refresh(conf, new HBasePolicyProvider());
    }
  }
}

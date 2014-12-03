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
package org.apache.hadoop.mapreduce.v2.app.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

/**
 * {@link PolicyProvider} for MapReduce history server protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ClientHSPolicyProvider extends PolicyProvider {
  
  private static final Service[] mrHSServices = 
      new Service[] {
    new Service(
        JHAdminConfig.MR_HS_SECURITY_SERVICE_AUTHORIZATION,
        HSClientProtocolPB.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_GET_USER_MAPPINGS,
        GetUserMappingsProtocol.class),
    new Service(
        CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_USER_MAPPINGS,
        RefreshUserMappingsProtocol.class),
    new Service(
        JHAdminConfig.MR_HS_SECURITY_SERVICE_AUTHORIZATION_ADMIN_REFRESH,
        HSAdminRefreshProtocol.class)
  };

  @Override
  public Service[] getServices() {
    return mrHSServices;
  }
}

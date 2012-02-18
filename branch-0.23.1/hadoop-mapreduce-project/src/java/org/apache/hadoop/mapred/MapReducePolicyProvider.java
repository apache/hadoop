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
package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.tools.GetUserMappingsProtocol;

/**
 * {@link PolicyProvider} for Map-Reduce protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapReducePolicyProvider extends PolicyProvider {
  private static final Service[] mapReduceServices = 
    new Service[] {
      new Service("security.inter.tracker.protocol.acl", 
                  InterTrackerProtocol.class),
      new Service("security.job.submission.protocol.acl",
                  ClientProtocol.class),
      new Service("security.task.umbilical.protocol.acl", 
                  TaskUmbilicalProtocol.class),
      new Service(
          CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_POLICY, 
          RefreshAuthorizationPolicyProtocol.class),
      new Service(
          CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_USER_MAPPINGS, 
          RefreshUserMappingsProtocol.class),
      new Service("security.admin.operations.protocol.acl", 
                  AdminOperationsProtocol.class),
      new Service(
          CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_AUTHORIZATION_GET_USER_MAPPINGS,
          GetUserMappingsProtocol.class)
  };
  
  @Override
  public Service[] getServices() {
    return mapReduceServices;
  }

}

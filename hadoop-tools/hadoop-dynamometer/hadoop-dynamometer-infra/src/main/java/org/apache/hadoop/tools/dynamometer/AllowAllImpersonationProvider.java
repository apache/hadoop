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
package org.apache.hadoop.tools.dynamometer;

import java.net.InetAddress;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ImpersonationProvider;

/**
 * An {@link ImpersonationProvider} that indiscriminately allows all users to
 * proxy as any other user.
 */
public class AllowAllImpersonationProvider extends Configured
    implements ImpersonationProvider {

  public void init(String configurationPrefix) {
    // Do nothing
  }

  public void authorize(UserGroupInformation user, InetAddress remoteAddress) {
    // Do nothing
  }

  // Although this API was removed from the interface by HADOOP-17367, we need
  // to keep it here because TestDynamometerInfra uses an old hadoop binary.
  public void authorize(UserGroupInformation user, String remoteAddress) {
    // Do nothing
  }
}

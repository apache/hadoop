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

package org.apache.hadoop.yarn.server.router.rmadmin;

import java.io.IOException;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;

/**
 * This class mocks the RMAdminRequestInterceptor.
 */
public class MockRMAdminRequestInterceptor
    extends DefaultRMAdminRequestInterceptor {

  MockRM mockRM = null;
  public void init(String user) {
    mockRM = new MockRM(super.getConf()) {
      @Override
      protected AdminService createAdminService() {
        return new AdminService(this) {
          @Override
          protected void startServer() {
            // override to not start rpc handler
          }

          @Override
          public RefreshServiceAclsResponse refreshServiceAcls(
              RefreshServiceAclsRequest request)
              throws YarnException, IOException {
            return RefreshServiceAclsResponse.newInstance();
          }

          @Override
          protected void stopServer() {
            // don't do anything
          }
        };
      }
    };
    mockRM.init(super.getConf());
    mockRM.start();
    super.setRMAdmin(mockRM.getAdminService());
  }

  @Override
  public void shutdown() {
    if (mockRM != null) {
      mockRM.stop();
    }
    super.shutdown();
  }
}

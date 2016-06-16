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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.queuing.QueuingContainerManagerImpl;

/**
 * Test class that invokes all test cases of {@link TestContainerManager} while
 * using the {@link QueuingContainerManagerImpl}. The goal is to assert that
 * no regression is introduced in the existing cases when no queuing of tasks at
 * the NMs is involved.
 */
public class TestContainerManagerRegression extends TestContainerManager {

  public TestContainerManagerRegression()
      throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LogFactory.getLog(TestContainerManagerRegression.class);
  }

  @Override
  protected ContainerManagerImpl createContainerManager(
      DeletionService delSrvc) {
    return new QueuingContainerManagerImpl(context, exec, delSrvc,
        nodeStatusUpdater, metrics, dirsHandler) {
      @Override
      public void
          setBlockNewContainerRequests(boolean blockNewContainerRequests) {
        // do nothing
      }

      @Override
      protected UserGroupInformation getRemoteUgi() throws YarnException {
        ApplicationId appId = ApplicationId.newInstance(0, 0);
        ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
            appId, 1);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(
            appAttemptId.toString());
        ugi.addTokenIdentifier(new NMTokenIdentifier(appAttemptId, context
            .getNodeId(), user, context.getNMTokenSecretManager()
                .getCurrentKey().getKeyId()));
        return ugi;
      }

      @Override
      protected void authorizeGetAndStopContainerRequest(
          ContainerId containerId, Container container, boolean stopRequest,
          NMTokenIdentifier identifier) throws YarnException {
        if (container == null || container.getUser().equals("Fail")) {
          throw new YarnException("Reject this container");
        }
      }
    };
  }
}

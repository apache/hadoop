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

package org.apache.hadoop.yarn.applications.distributedshell;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerLaunchFailAppMaster extends ApplicationMaster {

  private static final Logger LOG =
    LoggerFactory.getLogger(ContainerLaunchFailAppMaster.class);

  public ContainerLaunchFailAppMaster() {
    super();
  }

  @Override
  NMCallbackHandler createNMCallbackHandler() {
    return new FailContainerLaunchNMCallbackHandler(this);
  }

  class FailContainerLaunchNMCallbackHandler
    extends ApplicationMaster.NMCallbackHandler {

    public FailContainerLaunchNMCallbackHandler(
      ApplicationMaster applicationMaster) {
      super(applicationMaster);
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
                                   Map<String, ByteBuffer> allServiceResponse) {
      super.onStartContainerError(containerId,
        new RuntimeException("Inject Container Launch failure"));
    }

  }

  public static void main(String[] args) {
    boolean result = false;
    try {
      ContainerLaunchFailAppMaster appMaster =
        new ContainerLaunchFailAppMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.error("Error running ApplicationMaster", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

}

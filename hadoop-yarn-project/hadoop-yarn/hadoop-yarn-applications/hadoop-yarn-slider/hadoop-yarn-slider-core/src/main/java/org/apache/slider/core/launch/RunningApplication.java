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

package org.apache.slider.core.launch;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.client.SliderYarnClientImpl;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.server.appmaster.rpc.RpcBinder;

import java.io.IOException;

import static org.apache.slider.common.Constants.CONNECT_TIMEOUT;
import static org.apache.slider.common.Constants.RPC_TIMEOUT;

/**
 * A running application built from an app report. This one
 * can be talked to
 */
public class RunningApplication extends LaunchedApplication {

  private final ApplicationReport applicationReport;
  public RunningApplication(SliderYarnClientImpl yarnClient,
                            ApplicationReport applicationReport) {
    super(yarnClient, applicationReport);
    this.applicationReport = applicationReport;
  }

  public ApplicationReport getApplicationReport() {
    return applicationReport;
  }


  /**
   * Connect to a Slider AM
   * @param app application report providing the details on the application
   * @return an instance
   * @throws YarnException
   * @throws IOException
   */
  public SliderClusterProtocol connect(ApplicationReport app) throws
                                                             YarnException,
                                                             IOException {

    try {
      return RpcBinder.getProxy(yarnClient.getConfig(),
                                yarnClient.getRmClient(),
                                app,
                                CONNECT_TIMEOUT,
                                RPC_TIMEOUT);
    } catch (InterruptedException e) {
      throw new SliderException(SliderExitCodes.EXIT_TIMED_OUT,
          e,
          "Interrupted waiting for communications with the Application Master");
    }
  }

}

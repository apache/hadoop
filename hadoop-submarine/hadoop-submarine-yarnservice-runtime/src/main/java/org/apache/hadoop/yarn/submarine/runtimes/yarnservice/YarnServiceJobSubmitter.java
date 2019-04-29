/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowServiceSpec;
import org.apache.hadoop.yarn.submarine.utils.Localizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_SUCCESS;

/**
 * Submit a job to cluster.
 */
public class YarnServiceJobSubmitter implements JobSubmitter {

  private static final Logger LOG =
      LoggerFactory.getLogger(YarnServiceJobSubmitter.class);
  private ClientContext clientContext;
  private ServiceWrapper serviceWrapper;

  YarnServiceJobSubmitter(ClientContext clientContext) {
    this.clientContext = clientContext;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ApplicationId submitJob(RunJobParameters parameters)
      throws IOException, YarnException {
    FileSystemOperations fsOperations = new FileSystemOperations(clientContext);
    HadoopEnvironmentSetup hadoopEnvSetup =
        new HadoopEnvironmentSetup(clientContext, fsOperations);

    Service serviceSpec = createTensorFlowServiceSpec(parameters,
        fsOperations, hadoopEnvSetup);
    String serviceSpecFile = ServiceSpecFileGenerator.generateJson(serviceSpec);

    AppAdminClient appAdminClient =
        YarnServiceUtils.createServiceClient(clientContext.getYarnConfig());
    int code = appAdminClient.actionLaunch(serviceSpecFile,
        serviceSpec.getName(), null, null);
    if (code != EXIT_SUCCESS) {
      throw new YarnException(
          "Fail to launch application with exit code:" + code);
    }

    String appStatus=appAdminClient.getStatusString(serviceSpec.getName());
    Service app = ServiceApiUtil.jsonSerDeser.fromJson(appStatus);

    // Retry multiple times if applicationId is null
    int maxRetryTimes = 30;
    int count = 0;
    while (app.getId() == null && count < maxRetryTimes) {
      LOG.info("Waiting for application Id. AppStatusString=\n {}", appStatus);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      appStatus = appAdminClient.getStatusString(serviceSpec.getName());
      app = ServiceApiUtil.jsonSerDeser.fromJson(appStatus);
      count++;
    }
    // Retry timeout
    if (app.getId() == null) {
      throw new YarnException(
          "Can't get application id for Service " + serviceSpec.getName());
    }
    ApplicationId appid = ApplicationId.fromString(app.getId());
    appAdminClient.stop();
    return appid;
  }

  private Service createTensorFlowServiceSpec(RunJobParameters parameters,
      FileSystemOperations fsOperations, HadoopEnvironmentSetup hadoopEnvSetup)
      throws IOException {
    LaunchCommandFactory launchCommandFactory =
        new LaunchCommandFactory(hadoopEnvSetup, parameters,
            clientContext.getYarnConfig());
    Localizer localizer = new Localizer(fsOperations,
        clientContext.getRemoteDirectoryManager(), parameters);
    TensorFlowServiceSpec tensorFlowServiceSpec = new TensorFlowServiceSpec(
        parameters, this.clientContext, fsOperations, launchCommandFactory,
        localizer);

    serviceWrapper = tensorFlowServiceSpec.create();
    return serviceWrapper.getService();
  }

  @VisibleForTesting
  public ServiceWrapper getServiceWrapper() {
    return serviceWrapper;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.param.Quicklink;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.ServiceSpec;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.ServiceWrapper;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceUtils;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.LaunchCommandFactory;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component.TensorBoardComponent;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component.TensorFlowPsComponent;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component.TensorFlowWorkerComponent;
import org.apache.hadoop.yarn.submarine.utils.KerberosPrincipalFactory;
import org.apache.hadoop.yarn.submarine.utils.Localizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getDNSDomain;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.TensorFlowCommons.getUserName;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component.TensorBoardComponent.TENSORBOARD_QUICKLINK_LABEL;
import static org.apache.hadoop.yarn.submarine.utils.DockerUtilities.getDockerArtifact;
import static org.apache.hadoop.yarn.submarine.utils.EnvironmentUtilities.handleServiceEnvs;

/**
 * This class contains all the logic to create an instance
 * of a {@link Service} object for TensorFlow.
 * Worker,PS and Tensorboard components are added to the Service
 * based on the value of the received {@link RunJobParameters}.
 */
public class TensorFlowServiceSpec implements ServiceSpec {
  private static final Logger LOG =
      LoggerFactory.getLogger(TensorFlowServiceSpec.class);

  private final RemoteDirectoryManager remoteDirectoryManager;

  private final RunJobParameters parameters;
  private final Configuration yarnConfig;
  private final FileSystemOperations fsOperations;
  private final LaunchCommandFactory launchCommandFactory;
  private final Localizer localizer;

  public TensorFlowServiceSpec(RunJobParameters parameters,
      ClientContext clientContext, FileSystemOperations fsOperations,
      LaunchCommandFactory launchCommandFactory, Localizer localizer) {
    this.parameters = parameters;
    this.remoteDirectoryManager = clientContext.getRemoteDirectoryManager();
    this.yarnConfig = clientContext.getYarnConfig();
    this.fsOperations = fsOperations;
    this.launchCommandFactory = launchCommandFactory;
    this.localizer = localizer;
  }

  @Override
  public ServiceWrapper create() throws IOException {
    ServiceWrapper serviceWrapper = createServiceSpecWrapper();

    if (parameters.getNumWorkers() > 0) {
      addWorkerComponents(serviceWrapper);
    }

    if (parameters.getNumPS() > 0) {
      addPsComponent(serviceWrapper);
    }

    if (parameters.isTensorboardEnabled()) {
      createTensorBoardComponent(serviceWrapper);
    }

    // After all components added, handle quicklinks
    handleQuicklinks(serviceWrapper.getService());

    return serviceWrapper;
  }

  private ServiceWrapper createServiceSpecWrapper() throws IOException {
    Service serviceSpec = new Service();
    serviceSpec.setName(parameters.getName());
    serviceSpec.setVersion(String.valueOf(System.currentTimeMillis()));
    serviceSpec.setArtifact(getDockerArtifact(parameters.getDockerImageName()));

    KerberosPrincipal kerberosPrincipal = KerberosPrincipalFactory
        .create(fsOperations, remoteDirectoryManager, parameters);
    if (kerberosPrincipal != null) {
      serviceSpec.setKerberosPrincipal(kerberosPrincipal);
    }

    handleServiceEnvs(serviceSpec, yarnConfig, parameters.getEnvars());
    localizer.handleLocalizations(serviceSpec);
    return new ServiceWrapper(serviceSpec);
  }

  private void createTensorBoardComponent(ServiceWrapper serviceWrapper)
      throws IOException {
    TensorBoardComponent tbComponent = new TensorBoardComponent(fsOperations,
        remoteDirectoryManager, parameters, launchCommandFactory, yarnConfig);
    serviceWrapper.addComponent(tbComponent);

    addQuicklink(serviceWrapper.getService(), TENSORBOARD_QUICKLINK_LABEL,
        tbComponent.getTensorboardLink());
  }

  private static void addQuicklink(Service serviceSpec, String label,
      String link) {
    Map<String, String> quicklinks = serviceSpec.getQuicklinks();
    if (quicklinks == null) {
      quicklinks = new HashMap<>();
      serviceSpec.setQuicklinks(quicklinks);
    }

    if (SubmarineLogs.isVerbose()) {
      LOG.info("Added quicklink, " + label + "=" + link);
    }

    quicklinks.put(label, link);
  }

  private void handleQuicklinks(Service serviceSpec)
      throws IOException {
    List<Quicklink> quicklinks = parameters.getQuicklinks();
    if (quicklinks != null && !quicklinks.isEmpty()) {
      for (Quicklink ql : quicklinks) {
        // Make sure it is a valid instance name
        String instanceName = ql.getComponentInstanceName();
        boolean found = false;

        for (Component comp : serviceSpec.getComponents()) {
          for (int i = 0; i < comp.getNumberOfContainers(); i++) {
            String possibleInstanceName = comp.getName() + "-" + i;
            if (possibleInstanceName.equals(instanceName)) {
              found = true;
              break;
            }
          }
        }

        if (!found) {
          throw new IOException(
              "Couldn't find a component instance = " + instanceName
                  + " while adding quicklink");
        }

        String link = ql.getProtocol()
            + YarnServiceUtils.getDNSName(serviceSpec.getName(), instanceName,
                getUserName(), getDNSDomain(yarnConfig), ql.getPort());
        addQuicklink(serviceSpec, ql.getLabel(), link);
      }
    }
  }

  // Handle worker and primary_worker.

  private void addWorkerComponents(ServiceWrapper serviceWrapper)
      throws IOException {
    addWorkerComponent(serviceWrapper, parameters, TaskType.PRIMARY_WORKER);

    if (parameters.getNumWorkers() > 1) {
      addWorkerComponent(serviceWrapper, parameters, TaskType.WORKER);
    }
  }
  private void addWorkerComponent(ServiceWrapper serviceWrapper,
      RunJobParameters parameters, TaskType taskType) throws IOException {
    serviceWrapper.addComponent(
        new TensorFlowWorkerComponent(fsOperations, remoteDirectoryManager,
        parameters, taskType, launchCommandFactory, yarnConfig));
  }

  private void addPsComponent(ServiceWrapper serviceWrapper)
      throws IOException {
    serviceWrapper.addComponent(
        new TensorFlowPsComponent(fsOperations, remoteDirectoryManager,
            launchCommandFactory, parameters, yarnConfig));
  }

}
